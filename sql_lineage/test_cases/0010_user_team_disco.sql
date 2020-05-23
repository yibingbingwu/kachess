-- Hive syntax:
CREATE TABLE base_user_team AS
select
  _usr.col100l as user_id
, _usr.col101s as user_dim_1
, _usr.col102b as user_dim_2
, _team.col200l as team_id
, _team.col201s as team_dim_1
, _team.col202b as team_dim_2
from dw.tab10 as _usr
join dw.tab20 as _team
on _usr.col103i = _team.col200l
;


-- Conforming cases
CREATE TABLE conforming_case_1 AS
select count(1) col_dummy from (
    select t0.* from base_user_team t0
    join base_user_team t1
    ON t0.user_id=t1.user_id
     -- TODO: FAILED - should be non-conforming: and t0.team_id=7
     -- OK: and t0.team_id=func(t1.team_id, 23)
) z
;
