use bingql;

select count(1)=3 from (
select usage_context, count(1) cnt from (
select r.* from select_item s
join select_item_rel r on s.id=r.child_select_item_id
where s.alias='fv_col'
) z
group by 1
) z0
where (usage_context='JOIN' and cnt=2) OR (usage_context='SELECT' and cnt=2) OR (usage_context='WHERE' and cnt=2)

