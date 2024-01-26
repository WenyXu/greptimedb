// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::env;
use std::sync::Arc;

use common_telemetry::{error, info};
use rand::{Rng, SeedableRng};
use sqlx::mysql::{MySql, MySqlPoolOptions};
use sqlx::Pool;
use tests_fuzz::context::{TableContext, TableContextRef};
use tests_fuzz::fake::{
    merge_two_word_map_fn, random_capitalize_map, uppercase_and_keyword_backtick_map,
    MappedGenerator, WordGenerator,
};
use tests_fuzz::generator::create_expr::CreateTableExprGeneratorBuilder;
use tests_fuzz::generator::insert_expr::InsertExprGeneratorBuilder;
use tests_fuzz::generator::select_expr::SelectExprGeneratorBuilder;
use tests_fuzz::generator::Generator;
use tests_fuzz::ir::{CreateTableExpr, InsertIntoExpr, SelectExpr};
use tests_fuzz::translator::generic::insert_expr::InsertIntoExprTranslator;
use tests_fuzz::translator::generic::select_expr::SelectExprTranslator;
use tests_fuzz::translator::mysql::create_expr::CreateTableExprTranslator;
use tests_fuzz::translator::DslTranslator;

const GT_STANDALONE_MYSQL_ADDR: &str = "GT_STANDALONE_MYSQL_ADDR";

#[tokio::test]
async fn test_execution() {
    common_telemetry::init_default_ut_logging();
    let _ = dotenv::dotenv();
    let addr = if let Ok(addr) = env::var(GT_STANDALONE_MYSQL_ADDR) {
        addr
    } else {
        info!("GT_STANDALONE_MYSQL_ADDR is empty, ignores test");
        return;
    };

    let pool = MySqlPoolOptions::new()
        .connect(&format!("mysql://{addr}/public"))
        .await
        .unwrap();

    // let seed = rand::random();
    let seed = 8538676605000254751;
    info!("Test seed: {seed:?}");
    let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(seed);

    let create_table_expr = create_table(&mut rng);
    execute_create_table(&pool, &create_table_expr).await;

    let table_ctx = Arc::new(TableContext::from(&create_table_expr));
    let insert_expr = insert_into(&table_ctx, &mut rng);
    execute_insert_into(&pool, &insert_expr).await;

    let select_expr = select(&table_ctx, &mut rng);
    execute_select(&pool, &select_expr).await;
}

fn create_table<R: Rng + 'static>(rng: &mut R) -> CreateTableExpr {
    let create_table_generator = CreateTableExprGeneratorBuilder::default()
        .name_generator(Box::new(MappedGenerator::new(
            WordGenerator,
            merge_two_word_map_fn(random_capitalize_map, uppercase_and_keyword_backtick_map),
        )))
        .columns(rng.gen_range(1..10))
        .engine("mito")
        .build()
        .unwrap();

    create_table_generator.generate(rng).unwrap()
}

async fn execute_create_table(pool: &Pool<MySql>, create_table_expr: &CreateTableExpr) {
    let translator = CreateTableExprTranslator;
    let sql = translator.translate(create_table_expr).unwrap();
    info!("Creating table: {sql}");
    if let Err(err) = sqlx::query(&sql).execute(pool).await {
        error!(err; "Failed to execute: {sql}");
    }
}

fn insert_into<R: Rng + 'static>(table_ctx: &TableContextRef, rng: &mut R) -> InsertIntoExpr {
    let insert_into_generator = InsertExprGeneratorBuilder::default()
        .table_ctx(table_ctx.clone())
        .rows(rng.gen_range(1..20))
        .build()
        .unwrap();

    insert_into_generator.generate(rng).unwrap()
}

async fn execute_insert_into(pool: &Pool<MySql>, insert_into: &InsertIntoExpr) {
    let translator = InsertIntoExprTranslator;
    let sql = translator.translate(insert_into).unwrap();
    info!("Inserting: {sql}");

    match sqlx::query(&sql).execute(pool).await {
        Ok(result) => assert_eq!(result.rows_affected(), insert_into.rows.len() as u64),
        Err(err) => error!(err; "Failed to execute: {sql}"),
    }
}

fn select<R: Rng + 'static>(table_ctx: &TableContextRef, rng: &mut R) -> SelectExpr {
    let select_expr_generator = SelectExprGeneratorBuilder::default()
        .table_ctx(table_ctx.clone())
        .build()
        .unwrap();

    select_expr_generator.generate(rng).unwrap()
}

async fn execute_select(pool: &Pool<MySql>, select_expr: &SelectExpr) {
    let translator = SelectExprTranslator;
    let sql = translator.translate(select_expr).unwrap();
    info!("Selecting: {sql}");
    match sqlx::query(&sql).fetch_all(pool).await {
        Ok(result) => info!("{result:?}"),
        Err(err) => error!(err; "Failed to execute: {sql}"),
    }
}
