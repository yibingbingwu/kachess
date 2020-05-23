use bingql;
drop database if exists team_user_test;
create schema team_user_test;
use team_user_test;

create table user_id_parents
     select
	    sir.child_select_item_id as col_id
      from bingql.select_item_rel sir
      JOIN bingql.select_item sip -- JOINt to parent
        on sir.parent_select_item_id = sip.id
      where sir.usage_context = 'JOIN' and sip.definition like '%user_id%'
;
create table team_id_parents
     select
	    sir.child_select_item_id as col_id
      from bingql.select_item_rel sir
      JOIN bingql.select_item sip -- JOINt to parent
        on sir.parent_select_item_id = sip.id
      where sir.usage_context = 'JOIN' and sip.definition like '%team_id%'
;

create table have_user_but_not_team
      select u.col_id
      from user_id_parents u
      left JOIN team_id_parents t on u.col_id=t.col_id
      where t.col_id is NULL
;

select count(1)>0 as_expected from have_user_but_not_team;
