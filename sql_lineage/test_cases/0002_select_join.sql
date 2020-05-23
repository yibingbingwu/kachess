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
on a.col100l=k.col200l
WHERE a.col101s is not null
;
