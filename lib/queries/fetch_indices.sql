SELECT
    t.relname AS table_name,
    i.relname AS index_name,
    a.attname AS column_name,
    tc.constraint_type AS idx_type,
    ts.table_schema AS table_schema
FROM
    pg_class t
        LEFT OUTER JOIN information_schema.tables ts ON t.relname = ts.table_name,
    pg_class i
      LEFT OUTER JOIN information_schema.table_constraints tc ON i.relname = tc.constraint_name,
    pg_index ix,
    pg_attribute a

WHERE
    t.oid = ix.indrelid
    AND i.oid = ix.indexrelid
    AND a.attrelid = t.oid
    AND a.attnum = ANY(ix.indkey)
    AND t.relkind = 'r'
    AND t.relname NOT LIKE 'pg_%'
    AND (tc.table_name = t.relname OR tc.constraint_type IS NULL)
ORDER BY
    t.relname,
    i.relname;