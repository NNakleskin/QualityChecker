SELECT
    1
FROM {schema}.{table}
WHERE convert_to(to_char({column}), 'UTF8')::text <> to_char({column})
LIMIT 1;