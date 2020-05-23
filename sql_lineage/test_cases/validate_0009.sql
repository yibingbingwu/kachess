use bingql;

select count(1)=2 from (
select db_table, `type`, count(1) cnt from table_insert
group by 1, 2
) t0
where (db_table='dest_table_1' and type='INSERT OVERWRITE TABLE' and cnt=1) OR
(db_table='dest_table_2' and type='INSERT INTO TABLE' and cnt=2)
;

select parent_select_item_id=102 from select_item_rel
where usage_context='SELECT' and child_select_item_id in (
    select parent_select_item_id from select_item_rel
    where usage_context='SELECT' and child_select_item_id in (
        select i.id from select_item i JOIN  table_insert t
        where i.dataset_id=t.dataset_id and t.db_table='dest_table_1'
          and i.usage_context='SELECT' and i.name='yn_flag'
    )
)
;