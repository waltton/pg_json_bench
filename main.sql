CREATE TABLE tbl_text (data text);
CREATE TABLE tbl_json (data json);
CREATE TABLE tbl_jsonb (data jsonb);


CREATE TABLE tbl_btree_idx_score (data jsonb);
CREATE INDEX ON tbl_btree_idx_score (CAST(data->>'score' AS FLOAT));

CREATE TABLE tbl_gin_idx (data jsonb);
CREATE INDEX ON tbl_gin_idx USING GIN (data);

CREATE TABLE tbl_gin_idx_path (data jsonb);
CREATE INDEX ON tbl_gin_idx_path USING GIN (data jsonb_path_ops);


CREATE TABLE tbl_size_limit_text (data text);
CREATE TABLE tbl_size_limit_json (data json);
CREATE TABLE tbl_size_limit_jsonb (data jsonb);


CREATE TABLE tbl_test_toast_seed (data jsonb);
CREATE TABLE tbl_test_toast (title text, year int, score float, data jsonb);
