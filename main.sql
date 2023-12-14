CREATE EXTENSION IF NOT EXISTS pg_tde;

CREATE TABLE tbl_text (data text) USING heap;
CREATE TABLE tbl_json (data json) USING heap;
CREATE TABLE tbl_jsonb (data jsonb) USING heap;

CREATE TABLE tbl_text_tde (data text) USING pg_tde;
CREATE TABLE tbl_json_tde (data json) USING pg_tde;
CREATE TABLE tbl_jsonb_tde (data jsonb) USING pg_tde;


CREATE TABLE tbl_btree_idx_score (data jsonb);
CREATE INDEX ON tbl_btree_idx_score (CAST(data->>'score' AS FLOAT) ASC);

CREATE TABLE tbl_gin_idx (data jsonb);
CREATE INDEX ON tbl_gin_idx USING GIN (data);

CREATE TABLE tbl_gin_idx_path (data jsonb);
CREATE INDEX ON tbl_gin_idx_path USING GIN (data jsonb_path_ops);


CREATE TABLE tbl_size_limit_text (data text);
CREATE TABLE tbl_size_limit_json (data json);
CREATE TABLE tbl_size_limit_jsonb (data jsonb);


CREATE TABLE tbl_test_toast_seed (data jsonb);
CREATE TABLE tbl_test_toast (title text, year int, score float, data jsonb);
