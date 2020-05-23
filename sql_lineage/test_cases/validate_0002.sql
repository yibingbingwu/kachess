use bingql;
select count(1)=3 as ds_cnt_match from dataset where type='SUBQUERY';

-- Verify function parsing is correct
select count(1)=1 as func_parse_ok from select_item
where name='max_int' and definition='max(col103i, col303i)' and alias=name
and data_type is NULL;

-- Verify data_type pass through:
select count(1)=1 as bool_expr_ok from select_item
where name='yn_flag' and definition='col102b' and alias=name and data_type='BOOLEAN';

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
) and si.start_line=1
;

-- Test whether the correct usage context is parsed out:
select count(1)=3 from (
    select usage_context, count(1) cnt from select_item_rel where child_select_item_id in (select id from select_item where name='yn_flag') group by 1
) t0
where (usage_context='JOIN' and cnt=3) OR (usage_context='SELECT' and cnt=1) OR (usage_context='WHERE' and cnt=1)
;