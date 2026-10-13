CREATE OR REPLACE TABLE default.test_not_null (
    id INT,
    name STRING NOT NULL,
    address STRUCT<
        street: STRING NOT NULL,
        city: STRING NOT NULL,
        zip: STRING NOT NULL
    >,
    phone_numbers ARRAY<STRING>,
    metadata MAP<STRING, STRING>
);

INSERT INTO default.test_not_null VALUES (
    1,
    'Alice',
    NAMED_STRUCT('street', '123 Main St', 'city', 'Metropolis', 'zip', '12345'),
    ARRAY('123-456-7890', '987-654-3210'),
    MAP('age', '30', 'membership', 'gold')
);
