use bingql;
select count(1)=1 as only_one_new from dataset where type='SUBQUERY';

-- Verify the names and lineage:
select count(1)=4 as final_col_cnt_OK from dataset ds JOIN select_item as si on ds.id=si.dataset_id
where ds.type='SUBQUERY'
and si.name in ('col100l','col101s','col102b','col103i');

select count(1)=3 found_parents4col1 from dataset pds
JOIN select_item psi on pds.id=psi.dataset_id
JOIN select_item_rel sir on psi.id=sir.parent_select_item_id
JOIN (select si.id from dataset ds JOIN select_item as si on ds.id=si.dataset_id
      where ds.type='SUBQUERY'
      and si.name = 'col100l') ct ON ct.id=sir.child_select_item_id
WHERE concat(psi.map_to_schema, '.', psi.map_to_table, '.', psi.map_to_column) in
('dw.tab10.col100l', 'dw.tab20.col200l', 'stage.tab30.col300l')
;

select count(1)=2 found_parents4col3 from dataset pds
JOIN select_item psi on pds.id=psi.dataset_id
JOIN select_item_rel sir on psi.id=sir.parent_select_item_id
JOIN (select si.id from dataset ds JOIN select_item as si on ds.id=si.dataset_id
      where ds.type='SUBQUERY'
      and si.name = 'col102b') ct ON ct.id=sir.child_select_item_id
WHERE concat(psi.map_to_schema, '.', psi.map_to_table, '.', psi.map_to_column) in
('dw.tab10.col102b', 'dw.tab20.col202b')
;
