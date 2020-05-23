SELECT col101s as tc1, col201s as tc2, count(1) as cnt
FROM dw.tab10 t1
INNER JOIN dw.tab20 t2 ON t1.col100l=t2.col200l
GROUP BY 1, col201s
;