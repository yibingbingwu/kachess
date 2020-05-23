use bingql;

select count(1)=3 from (
SELECT t1.name, t1.usage_context, t1.map_to_table m2t
FROM select_item_rel t0
JOIN select_item t1 on t0.parent_select_item_id=t1.id
JOIN select_item t2 on t0.child_select_item_id=t2.id
WHERE t2.name='tc1'
) z0
where (usage_context='SELECT' and m2t='tab10' and name='col100l')
OR (usage_context='SELECT' and m2t='tab10' and name='col101s')
OR (usage_context='SELECT' and m2t='tab20' and name='col200l')
;
