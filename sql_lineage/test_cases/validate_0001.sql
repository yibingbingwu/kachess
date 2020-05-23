use bingql;
select count(1)=1 as ds_cnt_match from dataset where type='SUBQUERY';
select count(1)=4 as si_cnt_match from dataset ds JOIN select_item si on ds.id=si.dataset_id
  where ds.type='SUBQUERY';
select count(1)=4 as col_type_match from dataset ds JOIN select_item si on ds.id=si.dataset_id
  where ds.type='SUBQUERY'
    and ((si.name='col100l' and si.definition='col100l' and si.data_type='BIGINT')
	OR (si.name='col101s' and si.definition='col101s' and si.data_type='STRING')
	OR (si.name='col102b' and si.definition='col102b' and si.data_type='BOOLEAN')
	OR (name='col103i' and si.definition='col103i' and si.data_type='INT')
    )
;
