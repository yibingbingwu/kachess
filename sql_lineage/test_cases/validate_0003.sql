use bingql;
select count(1)=5 as ds_cnt_match from dataset where type='TABLE';

-- Tracing lineage:
-- This is the top level column:
select count(1)=1 lineage_ok from select_item_rel sr JOIN select_item si ON si.id=sr.child_select_item_id
where parent_select_item_id = (
    -- This is the column in the intermediate table/dataset:
    select sr.child_select_item_id from select_item_rel sr JOIN select_item si ON si.id=sr.child_select_item_id
    where parent_select_item_id = (
        -- Starting from the physical table, this is the immediate SELECT in the core:
        select child_select_item_id from select_item_rel sr JOIN select_item si ON si.id=sr.parent_select_item_id
        where si.name='col401s'
    ) and si.name='tab40_str'
)
;

select count(1)=1 fnd_table_create from table_insert;

select count(1)=3 confirm_insert from table_insert ti
JOIN select_item si on ti.dataset_id=si.dataset_id
WHERE si.name in ('max_int', 'yn_flag', 'col101s');