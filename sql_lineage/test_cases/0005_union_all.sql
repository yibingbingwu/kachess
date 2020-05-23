use adhoc;

select * from dw.tab10
UNION ALL
select col200l, col201s, col202b, col203i from dw.tab20
UNION ALL
select col300l, '2017-01-02', NULL, NULL from stage.tab30
;