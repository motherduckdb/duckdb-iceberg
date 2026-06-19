UPDATE default.spark_rewrite_mor_residual_deletes
SET payload = CASE
    WHEN id = 2 THEN 'c2_u'
    WHEN id = 101 THEN 'r101_u'
    ELSE payload
END
WHERE id IN (2, 101);
