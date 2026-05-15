# Region 状态与行为解耦重构方案

## 背景

`mito2` 当前使用 `RegionRoleState` 与 `RegionLeaderState` 表达 region 的角色和 leader 状态：

```rust
pub enum RegionLeaderState {
    Writable,
    Staging,
    EnteringStaging,
    Altering,
    Dropping,
    Truncating,
    Editing,
    Downgrading,
}

pub enum RegionRoleState {
    Leader(RegionLeaderState),
    Follower,
}
```

这套模型的问题是：`RegionLeaderState` 同时表达了两个概念：

1. **状态 / 阶段**：leader 当前正在做什么，例如 altering、editing、downgrading。
2. **操作行为**：遇到写入、flush、manifest update、compaction publish 等操作时应该如何处理。

例如：

- `Downgrading` 禁止写入，但允许 flush 和部分 manifest update。
- `Editing` 禁止写入，但允许 region edit 更新 manifest，也允许 compaction publish。
- `Staging` 可以接受写入，但可能因为 staging directive 是 `RejectAllWrites` 而拒绝写入。

这些语义现在分散在多个 `match RegionLeaderState` 或 helper 中。重构目标不是简单把这些 `match` 集中起来，而是要减少 **state/phase** 与 **role/operation behavior** 的耦合。

## 核心目标

重构后的模型应该满足：

1. `phase` 只表达 leader 当前所处阶段。
2. `behavior` 显式表达当前 leader 对各类操作的处理方式。
3. 不通过 `phase` 推导 `behavior`。
4. `Follower` 继续作为单独 variant，避免非法组合。
5. 迁移过程中保留旧代码和兼容层，所有测试通过后再考虑删除旧代码。

一句话：

```text
不是 phase -> behavior，
而是 LeaderState { phase, behavior }。
```

## 非目标

1. 不在第一阶段改变现有行为。
2. 不一次性删除旧 helper 或旧 enum。
3. 不把 index build 的历史 `should_abort_index()` 判断纳入核心行为模型。
4. 不引入 `role × phase` 的笛卡尔积模型。
5. 不为了“看起来统一”而把所有状态 helper 都做成 behavior。

## 推荐类型模型

### RegionRoleState

```rust
pub enum RegionRoleState {
    Leader(LeaderState),
    Follower,
}
```

`Follower` 仍然是单独 variant。

不要建模成：

```rust
pub struct RegionState {
    role: RegionRole,
    phase: LeaderPhase,
}
```

否则会产生非法组合：

```rust
Follower + Altering
Follower + Downgrading
Follower + Truncating
```

Follower 实际只有 normal/open 语义，不存在完整 leader lifecycle。

### LeaderState

```rust
pub struct LeaderState {
    pub phase: LeaderPhase,
    pub behavior: LeaderBehavior,
}
```

`LeaderState` 是 leader 的完整运行状态。

- `phase`：当前正在做什么。
- `behavior`：当前对操作如何处理。

### LeaderPhase

```rust
pub enum LeaderPhase {
    Writable,
    Staging,
    EnteringStaging,
    Altering,
    Editing,
    Truncating,
    Dropping,
    Downgrading,
}
```

`LeaderPhase` 只表达阶段，不表达权限或行为。

### LeaderBehavior

```rust
pub struct LeaderBehavior {
    pub write: WriteBehavior,
    pub flush: bool,
    pub manifest_update: ManifestUpdateBehavior,
    pub compaction_manifest_update: bool,
    pub schedule_compaction: bool,
    pub preload_cache: bool,
}
```

命名使用 `LeaderBehavior`，不用 `RegionPermission` / `LeaderPermission` / `LeaderAccess`。

原因：

- 这里不只是权限，也包含写请求如何处理，比如 stall。
- `Access` 容易让人想到 ACL / auth access control。
- `Permission` 不能很好表达 `WriteBehavior::Stall`。
- `LeaderBehavior` 表达的是当前 leader 对各类 operation 的行为配置。

### WriteBehavior

```rust
pub enum WriteBehavior {
    Accept,
    Stall,
    Reject,
}
```

写路径不是简单的允许 / 禁止，而是三种行为：

| Behavior | 含义 |
| --- | --- |
| `Accept` | 接受写入 |
| `Stall` | 暂存 / 阻塞，等 transient state 结束后继续处理 |
| `Reject` | 直接拒绝 |

这比 `WritePermission` 更准确。

### ManifestUpdateBehavior

```rust
pub enum ManifestUpdateBehavior {
    Deny,
    ExpectedPhase,
    InflightDuringDowngrade,
}
```

Manifest update 不能简单建模成 bool，因为它同时承担 operation guard。

含义：

| Behavior | 含义 |
| --- | --- |
| `Deny` | 不允许更新 manifest |
| `ExpectedPhase` | 允许更新，但要求当前 `phase == expected` |
| `InflightDuringDowngrade` | 用于 downgrading，允许已有后台任务提交 manifest |

`ExpectedPhase` 用于保护具体 operation：

- alter 只能在 `Altering` 提交。
- truncate 只能在 `Truncating` 提交。
- edit 只能在 `Editing` 提交。
- enter staging 只能在 `EnteringStaging` 提交。

`InflightDuringDowngrade` 用于显式表达当前已有语义：downgrading 禁止新写入，但允许已有 flush / compaction 等后台任务提交 manifest。这个能力不应该从 `Downgrading` phase 隐式推导出来，而应该是 `LeaderBehavior` 的显式配置。

## 行为接口

推荐在 `RegionRoleState` 上提供语义化接口。

### 写入行为

```rust
impl RegionRoleState {
    pub fn write_behavior(&self) -> WriteBehavior {
        match self {
            RegionRoleState::Leader(state) => state.behavior.write,
            RegionRoleState::Follower => WriteBehavior::Reject,
        }
    }
}
```

调用方：

```rust
match region.state().write_behavior() {
    WriteBehavior::Accept => { /* handle write */ }
    WriteBehavior::Stall => { /* push to stalled requests */ }
    WriteBehavior::Reject => { /* return RegionState error */ }
}
```

### Flush

```rust
pub fn can_flush(&self) -> bool {
    match self {
        RegionRoleState::Leader(state) => state.behavior.flush,
        RegionRoleState::Follower => false,
    }
}
```

当前不拆分 manual flush / auto flush。

现有代码里：

- 手动 flush 走 `is_flushable()`，允许 `Writable | Staging | Downgrading`。
- 自动 flush 实际通过 writable region 扫描，只覆盖 `Writable | Staging`。

迁移阶段先保持现状，不新增 `can_accept_manual_flush()` / `can_accept_auto_flush()`。

### Manifest update

```rust
pub fn can_update_manifest(&self, expected: LeaderPhase) -> bool {
    match self {
        RegionRoleState::Leader(state) => match state.behavior.manifest_update {
            ManifestUpdateBehavior::Deny => false,
            ManifestUpdateBehavior::ExpectedPhase => state.phase == expected,
            ManifestUpdateBehavior::InflightDuringDowngrade => {
                expected != LeaderPhase::Downgrading
            }
        },
        RegionRoleState::Follower => false,
    }
}
```

这里 `phase` 不是用来推导权限，而是作为 operation guard：调用方说“我正在提交 Altering 的 manifest update”，则当前 leader state 必须允许 manifest update，并且 phase 必须匹配。

### Compaction manifest update

```rust
pub fn can_update_manifest_for_compaction(&self) -> bool {
    match self {
        RegionRoleState::Leader(state) => state.behavior.compaction_manifest_update,
        RegionRoleState::Follower => false,
    }
}
```

Compaction publish 是一种 manifest update，但当前允许状态和普通 `expected phase` 检查不同：

- `Writable`
- `Editing`
- `Downgrading`

所以它保留独立行为字段：`compaction_manifest_update`。

### Compaction 调度

```rust
pub fn can_schedule_compaction(&self) -> bool {
    match self {
        RegionRoleState::Leader(state) => state.behavior.schedule_compaction,
        RegionRoleState::Follower => false,
    }
}
```

注意区分：

- `schedule_compaction`：是否允许新调度 compaction。
- `compaction_manifest_update`：已完成 compaction 是否允许 publish manifest。

这两个不是一个概念。

### Cache preload

```rust
pub fn can_preload_cache(&self) -> bool {
    match self {
        RegionRoleState::Leader(state) => state.behavior.preload_cache,
        RegionRoleState::Follower => true,
    }
}
```

Cache preload 只有一个使用点：`region/opener.rs::can_load_cache()`。它可以作为 `LeaderBehavior` 的一部分，但不应命名为 `LOAD_CACHE` capability。

### Phase helper

这些是状态辅助方法，不是权限 / 行为：

```rust
pub fn leader_phase(&self) -> Option<LeaderPhase>;
pub fn is_staging(&self) -> bool;
pub fn is_entering_staging(&self) -> bool;
pub fn is_downgrading(&self) -> bool;
pub fn is_follower(&self) -> bool;
```

它们只读取 role/phase，不读取 behavior。

## 不纳入 LeaderBehavior 的内容

### Index build

不建议加入：

```rust
index_build: IndexBuildBehavior
abort_index_build: bool
can_build_index()
```

原因：index build 和 flush / compaction 一样，核心正确性由最终 manifest update 的状态检查兜底。

当前 `should_abort_index()` 更像历史上的特殊前置过滤：

1. `handle_rebuild_index.rs` 入口使用 `writable_region_or()`，它会允许 `Staging`。
2. 后续 `should_abort_index()` 又把 `Staging` abort 掉。
3. 但 index build 完成后提交 manifest 时仍会调用 `update_manifest(Writable, ...)` 做状态检查。

因此，`should_abort_index()` 不应进入新的核心行为模型。

迁移建议：

- 第一阶段可以先不动 `should_abort_index()`，避免行为变化。
- 后续 cleanup 可以考虑将入口改为更精确的 selector，例如 `writable_non_staging_region()`，再删除调度前的 `should_abort_index()`。
- 最终 correctness 仍由 manifest update 状态检查保证。

### Staging 数据 helper

以下方法依赖 staging metadata / partition directive，不属于通用 behavior：

```rust
pub fn maybe_staging_partition_expr_str(&self) -> Option<String>;
pub fn expected_partition_expr_version(&self) -> u64;
pub fn reject_all_writes_in_staging(&self) -> bool;
```

它们应继续作为 `MitoRegion` 的 staging 语义 helper。

## Named constructors

不要提供：

```rust
LeaderPhase::behavior()
```

因为这会回到 `phase -> behavior` 的模式。

推荐提供 `LeaderState` 的 named constructors：

```rust
impl LeaderState {
    pub fn writable() -> Self;
    pub fn staging() -> Self;
    pub fn entering_staging() -> Self;
    pub fn altering() -> Self;
    pub fn editing() -> Self;
    pub fn truncating() -> Self;
    pub fn dropping() -> Self;
    pub fn downgrading() -> Self;
}
```

这些 constructor 的语义是“构造一个完整 leader state”，而不是“由 phase 推导 behavior”。

示例：

```rust
impl LeaderState {
    pub fn writable() -> Self {
        Self {
            phase: LeaderPhase::Writable,
            behavior: LeaderBehavior {
                write: WriteBehavior::Accept,
                flush: true,
                manifest_update: ManifestUpdateBehavior::ExpectedPhase,
                compaction_manifest_update: true,
                schedule_compaction: true,
                preload_cache: true,
            },
        }
    }

    pub fn downgrading() -> Self {
        Self {
            phase: LeaderPhase::Downgrading,
            behavior: LeaderBehavior {
                write: WriteBehavior::Reject,
                flush: true,
                manifest_update: ManifestUpdateBehavior::InflightDuringDowngrade,
                compaction_manifest_update: true,
                schedule_compaction: true,
                preload_cache: false,
            },
        }
    }
}
```

## 当前行为矩阵

第一阶段应保持当前行为。

| LeaderState constructor | write | flush | manifest_update | compaction_manifest_update | schedule_compaction | preload_cache |
| --- | --- | --- | --- | --- | --- | --- |
| `writable()` | `Accept` | true | `ExpectedPhase` | true | true | true |
| `staging()` | `Accept` | true | `ExpectedPhase` | false | false | true |
| `entering_staging()` | `Stall` | false | `ExpectedPhase` | false | false | true |
| `altering()` | `Stall` | false | `ExpectedPhase` | false | true\* | true |
| `editing()` | `Stall` | false | `ExpectedPhase` | true | true\* | true |
| `truncating()` | `Reject` | false | `ExpectedPhase` | false | true\* | false |
| `dropping()` | `Reject` | false | `Deny` | false | true\* | false |
| `downgrading()` | `Reject` | true | `InflightDuringDowngrade` | true | true\* | false |

\* 当前 `schedule_compaction()` 只显式跳过 `Staging | EnteringStaging`，所以为了保持行为不变，第一阶段其它 leader phase 暂时保留 true。后续如果要收紧，应单独讨论并单独测试。

## Region selector 调整建议

现有 selector：

- `writable_region()`
- `writable_region_or()`
- `writable_non_staging_region()`
- `staging_region()`
- `flushable_region_or()`
- `follower_region()`

迁移后可以逐步让它们基于新接口实现：

| Selector | 建议语义 |
| --- | --- |
| `writable_region()` | `write_behavior() == WriteBehavior::Accept` |
| `writable_non_staging_region()` | writable 且非 staging |
| `staging_region()` | `is_staging()` |
| `flushable_region_or()` | `can_flush()` |
| `follower_region()` | `is_follower()` |

## 迁移原则

重构必须采用兼容优先策略：

1. 先保留旧代码和新代码两套结构。
2. 先在接口层替换，不急着替换内部存储。
3. 每一步作为 checkpoint 给人工 review。
4. 每个 checkpoint 在 review 前必须运行：
   - `make fmt`
   - `make clippy`
   - 最小相关测试
5. 不允许修改已有测试 assert。
6. 过程中任何问题都要停下来问。
7. 所有测试通过并确认行为一致后，才考虑删除旧代码。

## 迁移步骤

### Checkpoint 1：新增类型与兼容转换，不改行为

新增：

```rust
LeaderState
LeaderPhase
LeaderBehavior
WriteBehavior
ManifestUpdateBehavior
```

暂时保留：

```rust
RegionRoleState::Leader(RegionLeaderState)
```

增加临时兼容转换：

```rust
impl RegionLeaderState {
    fn to_leader_state_for_compat(self) -> LeaderState;
}
```

注意：这个转换只是迁移桥，最终要删除。它不是最终设计中的 `phase -> behavior` API。

### Checkpoint 2：旧 helper 改为通过新接口实现

保留旧 helper：

```rust
is_writable()
is_flushable()
should_abort_index()
```

其中 `is_writable()` / `is_flushable()` 可以逐步通过新接口实现。

`should_abort_index()` 暂时可保留，但不进入 `LeaderBehavior`。

### Checkpoint 3：补行为等价测试

新增测试应验证：

- 旧 `is_writable()` 与新 `write_behavior()` 等价。
- 旧 `is_flushable()` 与新 `can_flush()` 等价。
- 普通 manifest update 行为保持不变。
- compaction manifest update 行为保持不变。
- cache preload 行为保持不变。

不修改已有测试 assert。

### Checkpoint 4：替换核心调用点

逐步替换：

- `worker/handle_write.rs`
- `worker/handle_flush.rs`
- `ManifestContext::update_manifest()`
- `ManifestContext::update_manifest_for_compaction()`
- `worker/handle_compaction.rs::schedule_compaction()`
- `region/opener.rs::can_load_cache()`

每批替换后都作为 checkpoint 验证。

### Checkpoint 5：内部存储迁移

当接口层替换和测试稳定后，再把内部存储从：

```rust
RegionRoleState::Leader(RegionLeaderState)
```

迁移成：

```rust
RegionRoleState::Leader(LeaderState)
```

状态切换点应显式选择完整 leader state：

```rust
RegionRoleState::Leader(LeaderState::downgrading())
```

而不是只设置 phase。

### Checkpoint 6：清理旧代码

只有在所有相关测试通过后，再清理：

- 删除 `RegionLeaderState`。
- 删除 `to_leader_state_for_compat()`。
- 删除或保留兼容 wrapper。
- 将旧命名收敛到 `LeaderPhase` / `LeaderState`。

## 风险与注意事项

### Downgrading

`Downgrading` 是最容易误改的状态。

它当前语义是：

- 禁止新写入。
- 允许 flush。
- 允许部分 manifest update。
- 允许 compaction manifest publish。
- 禁止 cache preload。

这些行为必须显式配置在 `LeaderBehavior` 中。

### Staging

`Staging` 不是简单 writable。

它：

- `write = Accept`
- 但还要经过 `reject_all_writes_in_staging()` 判断。
- 不调度 compaction。
- manifest update 写 staging manifest。

### Editing

`Editing`：

- 写入应该 stall。
- region edit 可以更新 manifest。
- compaction manifest publish 当前允许。

### Compaction schedule vs publish

必须区分：

- 新 compaction 是否可调度。
- 已完成 compaction 是否可提交 manifest。

这两个行为不能合并。

### Index build

`should_abort_index()` 不进入新行为模型。

后续可以单独 cleanup，但不应该污染 `LeaderBehavior`。

## 建议验证

每个 checkpoint 至少运行：

```bash
make fmt
make clippy
```

并运行最小相关测试，例如：

```bash
cargo nextest run -p mito2 <相关测试过滤条件>
```

如果测试命令或 feature 不适配，应停下来确认，不自动换大范围测试或自动修复。

## 总结

最终目标模型是：

```rust
RegionRoleState::Leader(LeaderState {
    phase: LeaderPhase::Downgrading,
    behavior: LeaderBehavior {
        write: WriteBehavior::Reject,
        flush: true,
        manifest_update: ManifestUpdateBehavior::InflightDuringDowngrade,
        compaction_manifest_update: true,
        schedule_compaction: true,
        preload_cache: false,
    },
})
```

这表达的是：

- 当前 leader phase 是 downgrading。
- 当前操作行为是显式配置的。
- phase 不再负责推导 behavior。

这才是本次重构希望达成的 state 与 role/operation behavior 解耦。
