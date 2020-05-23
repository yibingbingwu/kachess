use bingql;
select count(1)=1 as ds_cnt_match from dataset where type='LATERAL_VIEW';

-- Verify the name and lineage around LV:
select count(1)=1 as lv_col_match from dataset ds JOIN select_item as si on ds.id=si.dataset_id
where ds.type='LATERAL_VIEW' and si.definition='row_alias';

select count(1) found_parent from dataset pds
JOIN select_item psi on pds.id=psi.dataset_id
JOIN select_item_rel sir on psi.id=sir.parent_select_item_id
JOIN (select si.id from dataset ds JOIN select_item as si on ds.id=si.dataset_id
      where ds.type='LATERAL_VIEW' and si.definition='row_alias') ct ON ct.id=sir.child_select_item_id
WHERE pds.type='TABLE' and pds.map_to_table='tab40' and psi.name='col402x';