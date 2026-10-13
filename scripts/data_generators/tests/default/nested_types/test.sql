CREATE OR REPLACE TABLE default.nested_types (
    id INT,
    name STRING,
    address STRUCT<
        street: STRING,
        city: STRING,
        zip: STRING
    >,
    phone_numbers ARRAY<STRING>,
    metadata MAP<STRING, STRING>
);

INSERT INTO default.nested_types VALUES (
  1,
  'Alice',
  NAMED_STRUCT('street', '123 Main St', 'city', 'Metropolis', 'zip', '12345'),
  ARRAY('123-456-7890', '987-654-3210'),
  MAP('age', '30', 'membership', 'gold')
);
