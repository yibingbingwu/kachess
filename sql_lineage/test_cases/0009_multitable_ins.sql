create table dw.dest_table_1 (
  col101s String
, max_int Integer
, yn_flag Integer
, tab40_str String
)
;

create table dw.dest_table_2 (
  yn_flag Integer
)
;

WITH base_select AS (
    SELECT col101s, max(col103i, col303i) as max_int, col102b as yn_flag, tab40_str
    FROM dw.tab10 a
    JOIN stage.tab30 b ON a.col100l=b.col300l
    LEFT JOIN (
        SELECT c.*, d.* from dw.tab20 c
        JOIN (
        SELECT col400l, col403i, col401s as tab40_str from adhoc.tab40
        ) d
    ON c.col200l=d.col400l
    WHERE col202b and col403i=100
    ) k
    ON a.col100l=k.col200l
)

FROM base_select
INSERT OVERWRITE TABLE dw.dest_table_1
SELECT col101s, max_int, yn_flag, tab40_str
WHERE tab40_str IS NOT NULL
INSERT INTO TABLE dw.dest_table_2
SELECT yn_flag
WHERE tab40_str IS NULL
;

INSERT INTO TABLE dw.dest_table_2
SELECT col102b FROM dw.tab10;