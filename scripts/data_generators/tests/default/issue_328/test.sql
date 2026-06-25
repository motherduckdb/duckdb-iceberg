CREATE OR REPLACE TABLE default.issue_328(
  name string,
  meta struct<
    ts timestamp
  >
)
TBLPROPERTIES (
    'format-version'='2',
    'write.update.mode'='merge-on-read'
)
partitioned by (day(meta.ts));

insert into default.issue_328 values
('name1', struct(TIMESTAMP '2025-01-01 00:00:00')),
('name2', struct(TIMESTAMP '2025-01-02 00:00:00')),
('name3', struct(TIMESTAMP '2025-01-03 00:00:00'));
