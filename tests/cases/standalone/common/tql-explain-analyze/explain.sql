CREATE TABLE test(i DOUBLE, j TIMESTAMP TIME INDEX, k STRING PRIMARY KEY);

-- insert two points at 1ms and one point at 2ms
INSERT INTO test VALUES (1, 1, "a"), (1, 1, "b"), (2, 2, "a");

-- explain at 0s, 5s and 10s. No point at 0s.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
TQL EXPLAIN (0, 10, '5s') test;

-- 'lookback' parameter is not fully supported, the test has to be updated
-- explain at 0s, 5s and 10s. No point at 0s.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
TQL EXPLAIN (0, 10, '1s', '2s') test;

-- explain at 0s, 5s and 10s. No point at 0s.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
TQL EXPLAIN ('1970-01-01T00:00:00'::timestamp, '1970-01-01T00:00:00'::timestamp + '10 seconds'::interval, '5s') test;

-- explain verbose at 0s, 5s and 10s. No point at 0s.
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (elapsed_compute.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
TQL EXPLAIN VERBOSE (0, 10, '5s') test;

DROP TABLE test;
