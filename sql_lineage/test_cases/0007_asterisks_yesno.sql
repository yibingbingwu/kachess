SELECT c.*, d.*, concat(str(d.col400l), tab40_str) as fv_col
FROM dw.tab20 c
JOIN (
    SELECT col400l, col403i, col401s as tab40_str FROM adhoc.tab40
) d
ON c.col200l=d.col400l
WHERE col202b and col403i=100
;