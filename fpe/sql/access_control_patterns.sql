-- Authorized-view + entitlement-table patterns over tokenized data.
--
-- Reference copy with ${PROJECT}/${DATASET} placeholders; the runnable version
-- is created by fpe/scripts/setup_access_control.sh, and the shapes are
-- benchmarked by `sweep.py --phase access_control`.
--
-- Context this models: data arrives ALREADY tokenized from an upstream PEP.
-- BigQuery never sees plaintext at rest. Row- and column-level access is
-- enforced by authorized views joined to a lookup/entitlement table, and
-- detokenization happens through a remote function only for entitled data.
--
-- The whole point of this file: the natural way to write that view is the
-- slowest possible way to write it, by a factor of ~200x.
--
--   BigQuery disables batching inside short-circuiting expressions
--   (CASE/IF, MERGE ... WHEN MATCHED): "the calls field has exactly one
--   element". One HTTP round trip per row.
--   https://docs.cloud.google.com/bigquery/docs/remote-functions#limitations
--
-- Measured results are in docs/performance-tuning.md section 6. This file is
-- ordered to match that section's table: scenario by scenario, naive first.
--
--   Scenario                          Naive                        Fix
--   -------------------------------   --------------------------   ------------------------
--   1. Mask one column                A       v_ssn_case           B  v_ssn_union
--   2. Mask three columns             D-naive v_row_and_column_naive  D  v_row_and_column
--   3. Low-cardinality column         E-naive v_name_naive         E  v_name_dedup
--   4. Row-level only (fewer rows)    -- no counterpart --         C  v_ssn_rowfilter
--   5. Point lookup by plaintext      F-naive v_lookup_naive       F  v_lookup_by_token
--
-- Scenario 4 is not result-equivalent to the others: it removes rows rather
-- than masking a column, so it is an alternative to scenario 1, not a faster
-- way to get the same answer.


-- ---------------------------------------------------------------------------
-- Entitlement lookup table
-- ---------------------------------------------------------------------------
-- Real deployments key this off a group, not a single user_email.
--
-- ROW-level grant: which branches the user may see rows from.
-- COLUMN-level grant: which columns they may see in clear within those rows.
-- Here: branches 0 and 1 of 0..3 (50% of rows); ssn and name in clear, email
-- masked -- so both dimensions are genuinely exercised.
CREATE OR REPLACE TABLE `${PROJECT}.${DATASET}.entitlements` AS
SELECT
  SESSION_USER() AS user_email,
  b              AS branch_id,
  TRUE           AS can_see_ssn,
  FALSE          AS can_see_email,
  TRUE           AS can_see_name
FROM UNNEST([0, 1]) AS b;

-- Base view adding the row-level access dimension.
CREATE OR REPLACE VIEW `${PROJECT}.${DATASET}.v_tokenized_branched` AS
SELECT *, MOD(id, 4) AS branch_id
FROM `${PROJECT}.${DATASET}.pii_tokenized`;


-- ===========================================================================
-- SCENARIO 1 -- mask one column, all rows returned
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 1 NAIVE -- pattern A: conditional detokenization
-- ---------------------------------------------------------------------------
-- Returns every row; masks ssn where not entitled. Semantically what you want.
-- Performance: catastrophic. The remote function sits inside a CASE, so
-- BigQuery short-circuits, batching is disabled, and you get one HTTP request
-- PER ROW.
CREATE OR REPLACE VIEW `${PROJECT}.${DATASET}.v_ssn_case` AS
SELECT
  t.id,
  t.branch_id,
  CASE WHEN e.can_see_ssn IS TRUE
       THEN `${PROJECT}.${DATASET}`.fpe_decrypt_b5000(t.ssn, 'ssn')
       ELSE '***-**-****'
  END AS ssn
FROM `${PROJECT}.${DATASET}.v_tokenized_branched` t
LEFT JOIN `${PROJECT}.${DATASET}.entitlements` e
  ON e.user_email = SESSION_USER() AND e.branch_id = t.branch_id;

-- ---------------------------------------------------------------------------
-- 1 FIX -- pattern B: UNION ALL of two unconditional branches
-- ---------------------------------------------------------------------------
-- Result-equivalent to pattern A (same rows, same masking), but no conditional
-- wraps the remote function. Each branch applies it unconditionally to its own
-- row set, so batching survives.
CREATE OR REPLACE VIEW `${PROJECT}.${DATASET}.v_ssn_union` AS
SELECT
  t.id,
  t.branch_id,
  `${PROJECT}.${DATASET}`.fpe_decrypt_b5000(t.ssn, 'ssn') AS ssn
FROM `${PROJECT}.${DATASET}.v_tokenized_branched` t
JOIN `${PROJECT}.${DATASET}.entitlements` e
  ON e.user_email = SESSION_USER()
 AND e.branch_id = t.branch_id
 AND e.can_see_ssn
UNION ALL
SELECT
  t.id,
  t.branch_id,
  '***-**-****' AS ssn
FROM `${PROJECT}.${DATASET}.v_tokenized_branched` t
LEFT JOIN `${PROJECT}.${DATASET}.entitlements` e
  ON e.user_email = SESSION_USER()
 AND e.branch_id = t.branch_id
 AND e.can_see_ssn
WHERE e.branch_id IS NULL;


-- ===========================================================================
-- SCENARIO 2 -- mask three independently-governed columns
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 2 NAIVE -- pattern D-naive: one CASE per governed column
-- ---------------------------------------------------------------------------
-- The obvious extension of pattern A to several columns, and the obvious
-- disaster: three short-circuiting expressions, so three chances to drop to
-- one request per row.
--
-- Defined here only so the anti-pattern is reproducible and the file covers
-- every scenario in the doc. Do not build on it.
CREATE OR REPLACE VIEW `${PROJECT}.${DATASET}.v_row_and_column_naive` AS
SELECT
  t.id,
  t.branch_id,
  CASE WHEN e.can_see_ssn
       THEN `${PROJECT}.${DATASET}`.fpe_decrypt_b5000(t.ssn, 'ssn')
       ELSE '***-**-****' END AS ssn,
  CASE WHEN e.can_see_email
       THEN `${PROJECT}.${DATASET}`.fpe_decrypt_b5000(t.email, 'email')
       ELSE '***@***' END AS email,
  CASE WHEN e.can_see_name
       THEN `${PROJECT}.${DATASET}`.fpe_decrypt_b5000(t.name, 'name')
       ELSE '***' END AS name
FROM `${PROJECT}.${DATASET}.v_tokenized_branched` t
JOIN `${PROJECT}.${DATASET}.entitlements` e
  ON e.user_email = SESSION_USER() AND e.branch_id = t.branch_id;

-- ---------------------------------------------------------------------------
-- 2 FIX -- pattern D: row AND column control, linear in columns
-- ---------------------------------------------------------------------------
-- Pattern B works, but a UNION ALL per masked column means 2^N branches for N
-- independently-governed columns. Three columns is already eight branches, each
-- rescanning the base table.
--
-- This scales linearly instead: decode each column in its own CTE, filtered by
-- that column's grant, then LEFT JOIN the decoded values back by key. The
-- COALESCE that applies the mask operates on an already-materialised join
-- column, NOT on a remote function call -- so nothing short-circuits.
--
-- Row-level control is the `visible` CTE (one entitlement join, applied once).
-- Column-level control is the WHERE inside each per-column CTE. A user without
-- a column grant makes that CTE scan zero rows, so the function is never called
-- for it at all.
CREATE OR REPLACE VIEW `${PROJECT}.${DATASET}.v_row_and_column` AS
WITH visible AS (
  -- ROW level: the entitlement join. Everything downstream sees only these.
  SELECT t.id, t.branch_id, t.ssn, t.email, t.name
  FROM `${PROJECT}.${DATASET}.v_tokenized_branched` t
  JOIN `${PROJECT}.${DATASET}.entitlements` e
    ON e.user_email = SESSION_USER()
   AND e.branch_id  = t.branch_id
),
grants AS (
  -- Column grants are per-user, not per-row: collapse to a single row so the
  -- per-column predicates below are scalar, not another join.
  SELECT
    LOGICAL_OR(can_see_ssn)   AS ssn,
    LOGICAL_OR(can_see_email) AS email,
    LOGICAL_OR(can_see_name)  AS name
  FROM `${PROJECT}.${DATASET}.entitlements`
  WHERE user_email = SESSION_USER()
),
ssn_dec AS (
  SELECT v.id, `${PROJECT}.${DATASET}`.fpe_decrypt_b5000(v.ssn, 'ssn') AS val
  FROM visible v
  WHERE (SELECT ssn FROM grants)
),
email_dec AS (
  SELECT v.id, `${PROJECT}.${DATASET}`.fpe_decrypt_b5000(v.email, 'email') AS val
  FROM visible v
  WHERE (SELECT email FROM grants)
),
name_dec AS (
  SELECT v.id, `${PROJECT}.${DATASET}`.fpe_decrypt_b5000(v.name, 'name') AS val
  FROM visible v
  WHERE (SELECT name FROM grants)
)
SELECT
  v.id,
  v.branch_id,
  COALESCE(s.val, '***-**-****') AS ssn,
  COALESCE(m.val, '***@***')     AS email,
  COALESCE(n.val, '***')         AS name
FROM visible v
LEFT JOIN ssn_dec   s USING (id)
LEFT JOIN email_dec m USING (id)
LEFT JOIN name_dec  n USING (id);


-- ===========================================================================
-- SCENARIO 3 -- low-cardinality column
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 3 NAIVE -- pattern E-naive: decode every row
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW `${PROJECT}.${DATASET}.v_name_naive` AS
SELECT
  t.id,
  `${PROJECT}.${DATASET}`.fpe_decrypt_b5000(t.name, 'name') AS name
FROM `${PROJECT}.${DATASET}.v_tokenized_branched` t
JOIN `${PROJECT}.${DATASET}.entitlements` e
  ON e.user_email = SESSION_USER()
 AND e.branch_id  = t.branch_id
 AND e.can_see_name;

-- ---------------------------------------------------------------------------
-- 3 FIX -- pattern E: decode DISTINCT tokens, then join back
-- ---------------------------------------------------------------------------
-- Determinism means equal plaintext always yields equal ciphertext, so a column
-- with C distinct values across R rows only needs C decryptions, not R. For a
-- low-cardinality column (name: ~100 distinct over 1,000,000 rows) that is a
-- four-order-of-magnitude reduction in remote-function work.
--
-- Only worth it when C << R. For near-unique columns (ssn, email) the DISTINCT
-- and the join cost more than they save -- the benchmark shows both cases.
CREATE OR REPLACE VIEW `${PROJECT}.${DATASET}.v_name_dedup` AS
WITH visible AS (
  SELECT t.id, t.name
  FROM `${PROJECT}.${DATASET}.v_tokenized_branched` t
  JOIN `${PROJECT}.${DATASET}.entitlements` e
    ON e.user_email = SESSION_USER()
   AND e.branch_id  = t.branch_id
   AND e.can_see_name
),
decoded AS (
  SELECT tok, `${PROJECT}.${DATASET}`.fpe_decrypt_b5000(tok, 'name') AS clear
  FROM (SELECT DISTINCT name AS tok FROM visible)
)
SELECT v.id, d.clear AS name
FROM visible v
JOIN decoded d ON d.tok = v.name;


-- ===========================================================================
-- SCENARIO 4 -- row-level control only (fewer rows, not masked columns)
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 4 -- pattern C: entitlement as a join predicate
-- ---------------------------------------------------------------------------
-- Non-entitled rows disappear entirely rather than being masked. Fast, because
-- the function only ever sees rows the user may read. Use when row-level
-- filtering is the required semantics; it is NOT a drop-in replacement for
-- pattern A, and so has no naive counterpart to beat.
CREATE OR REPLACE VIEW `${PROJECT}.${DATASET}.v_ssn_rowfilter` AS
SELECT
  t.id,
  t.branch_id,
  `${PROJECT}.${DATASET}`.fpe_decrypt_b5000(t.ssn, 'ssn') AS ssn
FROM `${PROJECT}.${DATASET}.v_tokenized_branched` t
JOIN `${PROJECT}.${DATASET}.entitlements` e
  ON e.user_email = SESSION_USER()
 AND e.branch_id = t.branch_id
 AND e.can_see_ssn;


-- ===========================================================================
-- SCENARIO 5 -- point lookup by plaintext, through an authorized view
--
-- Combines section 5 (search by tokenizing the term) with section 6 (access
-- control). This is the shape most real user-facing traffic takes: "show me
-- this one person's record, if I am allowed to see it".
--
-- The tension: an authorized view must detokenize to be useful, but a lookup
-- must filter before detokenizing or it decrypts the whole table. Resolving it
-- needs one idea -- the view exposes the TOKEN alongside the decrypted value,
-- so callers can filter on ciphertext. Section 4 established that BigQuery
-- pushes simple WHERE predicates through a remote function, so a predicate on
-- the token column is applied before the decryption in the projection.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 5 NAIVE -- pattern F-naive: filter on the decrypted output
-- ---------------------------------------------------------------------------
-- The intuitive query against pattern C's view. The predicate references the
-- view's decrypted column, so every entitled row must be decrypted before the
-- comparison can happen.
--
--   SELECT * FROM v_lookup_naive WHERE ssn = '123-45-6789';
--
CREATE OR REPLACE VIEW `${PROJECT}.${DATASET}.v_lookup_naive` AS
SELECT
  t.id,
  t.branch_id,
  `${PROJECT}.${DATASET}`.fpe_decrypt_b5000(t.ssn, 'ssn') AS ssn
FROM `${PROJECT}.${DATASET}.v_tokenized_branched` t
JOIN `${PROJECT}.${DATASET}.entitlements` e
  ON e.user_email = SESSION_USER()
 AND e.branch_id = t.branch_id
 AND e.can_see_ssn;

-- ---------------------------------------------------------------------------
-- 5 FIX -- pattern F: expose the token so callers can filter on ciphertext
-- ---------------------------------------------------------------------------
-- Identical to F-naive except it also projects the raw token as `ssn_token`.
-- That single extra column is what lets a caller search without decrypting:
--
--   DECLARE tok STRING;
--   SET tok = (SELECT `${PROJECT}.${DATASET}`.fpe_encrypt('123-45-6789','ssn'));
--   SELECT * FROM v_lookup_by_token WHERE ssn_token = tok;
--
-- One remote call to tokenize the term, entitlement still enforced by the join,
-- and only the matching row is decrypted.
--
-- Exposing the token is safe in the sense that ciphertext is not plaintext, but
-- it is not free: under deterministic tokenization a token is a stable
-- pseudonymous identifier that permits correlation. Expose it only where the
-- caller is already trusted with the row.
CREATE OR REPLACE VIEW `${PROJECT}.${DATASET}.v_lookup_by_token` AS
SELECT
  t.id,
  t.branch_id,
  t.ssn AS ssn_token,
  `${PROJECT}.${DATASET}`.fpe_decrypt_b5000(t.ssn, 'ssn') AS ssn
FROM `${PROJECT}.${DATASET}.v_tokenized_branched` t
JOIN `${PROJECT}.${DATASET}.entitlements` e
  ON e.user_email = SESSION_USER()
 AND e.branch_id = t.branch_id
 AND e.can_see_ssn;


-- ---------------------------------------------------------------------------
-- Hardening notes
-- ---------------------------------------------------------------------------
-- 1. Put the remote FUNCTIONS in a dataset users cannot query. Anyone who can
--    call fpe_decrypt directly bypasses every view above. Expose only the
--    views; authorize them onto the source and routine datasets.
-- 2. Detokenize LAST. Joins, GROUP BY, filters and counts all run natively on
--    tokens at zero remote-function cost -- see the `placement` benchmark.
-- 3. Every query through these views counts against the per-project limit of
--    10 concurrent queries containing remote functions. Scope the detokenizing
--    views to a small entitled population and request a quota increase early.
-- 4. A lookup whose token the caller already holds contains no remote function
--    at all, so it does not count against that limit -- see scenario 5 and
--    docs/performance-tuning.md section 5.
