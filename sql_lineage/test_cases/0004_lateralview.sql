use adhoc;
SELECT row_alias.user.id, count(1) as cnt
FROM tab40
LATERAL VIEW explode(col402x) ds_alias AS row_alias
GROUP BY 1
;