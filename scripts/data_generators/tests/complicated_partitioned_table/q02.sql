insert into default.complicated_partitioned_table
select id, make_timestamp(2025, 01, (id%31) + 1, 0, 0, 0), 'new values', bround(random()*1000, 0)  from range(0, 1000);