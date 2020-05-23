use adhoc;

create table if not exists created_tab_0 (
  id bigint
, max_int integer COMMENT 'This text should not show up'
, yn_flag boolean
, tab40_str string
)
COMMENT 'This is a test'
PARTITIONED BY
(
        ds string
)
stored as PARQUET
location 's3://somewhere/in/the/ether';

INSERT OVERWRITE TABLE adhoc.created_tab_0
PARTITION (ds='2018-01-01')
SELECT col101s, max(col103i, col303i) as max_int, col102b as yn_flag, tab40_str
FROM dw.tab10 a
JOIN stage.tab30 b ON a.col100l=b.col300l
LEFT JOIN (
  SELECT c.*, d.* from dw.tab20 c
  JOIN (
    SELECT col400l, col403i, col401s as tab40_str from adhoc.tab40
  ) d
on c.col200l=d.col400l
WHERE col202b and col403i=100
) k
on a.col100l=k.col200l
WHERE a.col101s is not null
;
