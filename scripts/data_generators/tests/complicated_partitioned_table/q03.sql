insert into default.complicated_partitioned_table
select id, make_timestamp(2025, (id%12 + 1), (id%28) + 1, 0, 0, 0), 'new values', bround(random()*10000, 0) from range(0, 1000000);