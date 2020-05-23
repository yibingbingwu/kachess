use adhoc;

-- This test case is created after I realized that I didn't
-- include the asterisk in count(*) and the cols in WHERE or JOIN
create table test_tab101 as
SELECT  col103i , base_cnt
FROM dw.tab10 a
JOIN (
    SELECT col400l, count(*) as base_cnt
    FROM adhoc.tab40
    WHERE col401s LIKE '%abc%' AND abs(col403i) between 3 and 10
) b ON a.col100l=b.col400l
;