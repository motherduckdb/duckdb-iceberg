copy (select * from read_avro('s3://warehouse/default/large_partitioned_table_for_vacuum/metadata/dd54fbe4-e8fe-4534-9c4c-5e8ed1bdaa0b-m1.avro') where manifests3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1984-09/00000-42155-dd1b4b4f-f833-4604-91a8-dba89c7f76ff-0-00001.parquet
copy (select * from 's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1984-09/00000-42167-732c9827-ec5e-44f2-b2a3-1a919fd2f708-0-00001.parquet') to 'data/persistent/large_partitioned_table/data/joined_month=1984-09/00000-42167-732c9827-ec5e-44f2-b2a3-1a919fd2f708-0-00001.parquet' (FIELD_IDS {'id': 1, 'name': 2, 'joined': 3, 'joined_month': 1000});
copy (select * from 's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1984-09/00000-42179-88be9947-7b02-4fd4-b3d4-c794944129c7-0-00001.parquet') to 'data/persistent/large_partitioned_table/data/joined_month=1984-09/00000-42179-88be9947-7b02-4fd4-b3d4-c794944129c7-0-00001.parquet' (FIELD_IDS {'id': 1, 'name': 2, 'joined': 3, 'joined_month': 1000});
copy (select * from 's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1984-10/00000-43055-fba934f2-9233-4232-b0ee-c2590d27b5a1-0-00001.parquet') to 'data/persistent/large_partitioned_table/data/joined_month=1984-10/00000-43055-fba934f2-9233-4232-b0ee-c2590d27b5a1-0-00001.parquet' (FIELD_IDS {'id': 1, 'name': 2, 'joined': 3, 'joined_month': 1000});
copy (select * from 's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1984-10/00000-43067-4f95304c-c723-4687-8866-b10c02e6a2f1-0-00001.parquet') to 'data/persistent/large_partitioned_table/data/joined_month=1984-10/00000-43067-4f95304c-c723-4687-8866-b10c02e6a2f1-0-00001.parquet' (FIELD_IDS {'id': 1, 'name': 2, 'joined': 3, 'joined_month': 1000});
copy (select * from 's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1984-10/00000-43079-941f1f20-929b-4fef-a1c4-3ddd0cc73a72-0-00001.parquet') to 'data/persistent/large_partitioned_table/data/joined_month=1984-10/00000-43079-941f1f20-929b-4fef-a1c4-3ddd0cc73a72-0-00001.parquet' (FIELD_IDS {'id': 1, 'name': 2, 'joined': 3, 'joined_month': 1000});
copy (select * from 's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1984-11/00000-43703-e2078303-362e-4718-91e5-c7164c053240-0-00001.parquet') to 'data/persistent/large_partitioned_table/data/joined_month=1984-11/00000-43703-e2078303-362e-4718-91e5-c7164c053240-0-00001.parquet' (FIELD_IDS {'id': 1, 'name': 2, 'joined': 3, 'joined_month': 1000});
copy (select * from 's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1984-11/00000-43715-e9637c83-d2f0-4dd3-94bd-f6bd9381cf7f-0-00001.parquet') to 'data/persistent/large_partitioned_table/data/joined_month=1984-11/00000-43715-e9637c83-d2f0-4dd3-94bd-f6bd9381cf7f-0-00001.parquet' (FIELD_IDS {'id': 1, 'name': 2, 'joined': 3, 'joined_month': 1000});
copy (select * from 's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1984-11/00000-43727-a12cb232-8467-4828-9a1b-de3dda8b690d-0-00001.parquet') to 'data/persistent/large_partitioned_table/data/joined_month=1984-11/00000-43727-a12cb232-8467-4828-9a1b-de3dda8b690d-0-00001.parquet' (FIELD_IDS {'id': 1, 'name': 2, 'joined': 3, 'joined_month': 1000});
copy (select * from 's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1984-12/00000-44423-e0aab5b5-449d-48fc-aba3-427bae1a60fd-0-00001.parquet') to 'data/persistent/large_partitioned_table/data/joined_month=1984-12/00000-44423-e0aab5b5-449d-48fc-aba3-427bae1a60fd-0-00001.parquet' (FIELD_IDS {'id': 1, 'name': 2, 'joined': 3, 'joined_month': 1000});
copy (select * from 's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1984-12/00000-44435-af026708-5ef2-4c83-813b-4ac5cb83db9b-0-00001.parquet') to 'data/persistent/large_partitioned_table/data/joined_month=1984-12/00000-44435-af026708-5ef2-4c83-813b-4ac5cb83db9b-0-00001.parquet' (FIELD_IDS {'id': 1, 'name': 2, 'joined': 3, 'joined_month': 1000});
copy (select * from 's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1984-12/00000-44447-4a9ecf35-60d4-474c-a762-0aa6106130bc-0-00001.parquet') to 'data/persistent/large_partitioned_table/data/joined_month=1984-12/00000-44447-4a9ecf35-60d4-474c-a762-0aa6106130bc-0-00001.parquet' (FIELD_IDS {'id': 1, 'name': 2, 'joined': 3, 'joined_month': 1000});
copy (select * from 's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1985-01/00000-45095-b2d94550-f961-470d-a2ab-f73a1dd0ec3c-0-00001.parquet') to 'data/persistent/large_partitioned_table/data/joined_month=1985-01/00000-45095-b2d94550-f961-470d-a2ab-f73a1dd0ec3c-0-00001.parquet' (FIELD_IDS {'id': 1, 'name': 2, 'joined': 3, 'joined_month': 1000});
copy (select * from 's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1985-01/00000-45107-33b1ffa7-56b2-4e08-bcec-c5f419abcebd-0-00001.parquet') to 'data/persistent/large_partitioned_table/data/joined_month=1985-01/00000-45107-33b1ffa7-56b2-4e08-bcec-c5f419abcebd-0-00001.parquet' (FIELD_IDS {'id': 1, 'name': 2, 'joined': 3, 'joined_month': 1000});
copy (select * from 's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1985-01/00000-45119-ddf88af7-b61d-4cec-8355-63f8d84728be-0-00001.parquet') to 'data/persistent/large_partitioned_table/data/joined_month=1985-01/00000-45119-ddf88af7-b61d-4cec-8355-63f8d84728be-0-00001.parquet' (FIELD_IDS {'id': 1, 'name': 2, 'joined': 3, 'joined_month': 1000});
copy (select * from 's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1985-01/00000-45131-54da56a5-7ea8-424e-a013-39c877892798-0-00001.parquet') to 'data/persistent/large_partitioned_table/data/joined_month=1985-01/00000-45131-54da56a5-7ea8-424e-a013-39c877892798-0-00001.parquet' (FIELD_IDS {'id': 1, 'name': 2, 'joined': 3, 'joined_month': 1000});



copy (
select 
	status, 
	snapshot_id, 
	sequence_number,
	file_sequence_number,
	data_file 
from 
	(select * from (
		select 
			* replace(struct_update(data_file, file_path := ('data/persistent/large_partitioned_table/data' || data_file.file_path[63:])) as data_file),
			unnest(data_file)
		from read_avro('s3://warehouse/default/large_partitioned_table_for_vacuum/metadata/dd54fbe4-e8fe-4534-9c4c-5e8ed1bdaa0b-m1.avro')
	)
where file_path in (
	's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1984-09/00000-42167-732c9827-ec5e-44f2-b2a3-1a919fd2f708-0-00001.parquet',
	's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1984-09/00000-42179-88be9947-7b02-4fd4-b3d4-c794944129c7-0-00001.parquet',
	's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1984-10/00000-43055-fba934f2-9233-4232-b0ee-c2590d27b5a1-0-00001.parquet',
	's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1984-10/00000-43067-4f95304c-c723-4687-8866-b10c02e6a2f1-0-00001.parquet',
	's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1984-10/00000-43079-941f1f20-929b-4fef-a1c4-3ddd0cc73a72-0-00001.parquet',
	's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1984-11/00000-43703-e2078303-362e-4718-91e5-c7164c053240-0-00001.parquet',
	's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1984-11/00000-43715-e9637c83-d2f0-4dd3-94bd-f6bd9381cf7f-0-00001.parquet',
	's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1984-11/00000-43727-a12cb232-8467-4828-9a1b-de3dda8b690d-0-00001.parquet',
	's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1984-12/00000-44423-e0aab5b5-449d-48fc-aba3-427bae1a60fd-0-00001.parquet',
	's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1984-12/00000-44435-af026708-5ef2-4c83-813b-4ac5cb83db9b-0-00001.parquet',
	's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1984-12/00000-44447-4a9ecf35-60d4-474c-a762-0aa6106130bc-0-00001.parquet',
	's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1985-01/00000-45095-b2d94550-f961-470d-a2ab-f73a1dd0ec3c-0-00001.parquet',
	's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1985-01/00000-45107-33b1ffa7-56b2-4e08-bcec-c5f419abcebd-0-00001.parquet',
	's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1985-01/00000-45119-ddf88af7-b61d-4cec-8355-63f8d84728be-0-00001.parquet',
	's3://warehouse/default/large_partitioned_table_for_vacuum/data/joined_month=1985-01/00000-45131-54da56a5-7ea8-424e-a013-39c877892798-0-00001.parquet'
)))
to 
'data/persistent/large_partitioned_table/metadata/dd54fbe4-e8fe-4534-9c4c-5e8ed1bdaa0b-m1.avro' (FORMAT AVRO,
FIELD_IDS {
	'status': 0,
	'snapshot_id' : 1,
	'sequence_number' : 3,
	'file_sequence_number' : 4,
	'data_file': {
		'__duckdb_field_id': 2,
		'content': 134,
		'file_path' : 100,
		'file_format' : 101,
		'partition': {
			'__duckdb_field_id' : 102,
			'joined_month': 1000
		},
		'record_count' : 103,
		'file_size_in_bytes': 104,
		'column_sizes': {
			'__duckdb_field_id': 108,
			'key' : 117,
			'value': 118
		},
		'value_counts' : {
			'__duckdb_field_id' : 109,
			'key': 119,
			'value': 120
		},
		'null_value_counts': {
			'__duckdb_field_id' : 110,
			'key': 121,
			'value': 122
		},
		'nan_value_counts': {
			'__duckdb_field_id' : 137,
			'key': 138,
			'value': 139
		},
		'lower_bounds': {
			'__duckdb_field_id' : 125,
			'key': 126,
			'value': 127
		},
		'upper_bounds': {
			'__duckdb_field_id' : 128,
			'key': 129,
			'value': 130
		},
		'key_metadata': 131,
		'split_offsets': {
			'__duckdb_field_id': 132,
			'element': 133
		}, 
		'equality_ids': {
			'__duckdb_field_id': 135,
			'element': 136
		},
		'sort_order_id': 140,
		'referenced_data_file': 143

	}
});




copy (
	select 
		* replace('data/persistent/large_partitioned_table/metadata/dd54fbe4-e8fe-4534-9c4c-5e8ed1bdaa0b-m1.avro'as manifest_path) 
	from 
		read_avro('s3://warehouse/default/large_partitioned_table_for_vacuum/metadata/snap-6252360244030588538-1-0f2ad419-3d04-47ee-8a2c-ac3cef5c5d05.avro') where manifest_length = 202936
) to 'data/persistent/large_partitioned_table/metadata/snap-6252360244030588538-1-0f2ad419-3d04-47ee-8a2c-ac3cef5c5d05.avro' 
(FORMAT AVRO, FIELD_IDS {
	'manifest_path': 500,
	'manifest_length': 501,
	'partition_spec_id': 502,
	'content': 517,
	'sequence_number': 515,
	'min_sequence_number': 516,
	'added_snapshot_id': 503,
	'added_files_count': 504,
	'existing_files_count': 505,
	'deleted_files_count': 506,
	'added_rows_count': 512,
	'existing_rows_count': 513,
	'deleted_rows_count': 514,
	'partitions': {
		'element': {
			'contains_null': 509,
			'contains_nan': 518,
			'lower_bound': 510,
			'upper_bound': 511,
			'__duckdb_field_id': 508
		},
		'__duckdb_field_id': 507
	},
	'key_metadata': 519
});



