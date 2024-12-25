{{
    config(
        materialized    = 'incremental',
        post_hook       = ["truncate table ahsokatano_4_raw.xyz_previous_schema_state"],
        tags            = ['schema', 'warehouse'],
    )
}}

SELECT 
    CURRENT_TIMESTAMP AS change_time,
    c.table_catalog AS table_catalog,
    c.table_schema AS table_schema,
    c.table_name AS table_name,
    c.column_name AS column_name,
    c.data_type AS data_type,
    COALESCE(CAST(c.character_maximum_length AS VARCHAR), 'NA1') AS character_maximum_length,
    COALESCE(CAST(c.numeric_precision AS VARCHAR), 'NA2') AS numeric_precision,
    COALESCE(CAST(c.numeric_scale AS VARCHAR), 'NA3') AS numeric_scale
FROM 
    INFORMATION_SCHEMA.COLUMNS c
LEFT JOIN 
    ahsokatano_4_raw.xyz_previous_schema_state p 
ON 
    c.table_catalog = p.table_catalog 
    AND c.table_schema = p.table_schema
    AND c.table_name = p.table_name
    AND c.column_name = p.column_name
    AND c.data_type = p.data_type
    AND COALESCE(CAST(c.character_maximum_length AS VARCHAR), 'NA1') = p.character_maximum_length
    AND COALESCE(CAST(c.numeric_precision AS VARCHAR), 'NA2') = p.numeric_precision
    AND COALESCE(CAST(c.numeric_scale AS VARCHAR), 'NA3') = p.numeric_scale
WHERE 
    c.table_schema IN ('PADME', 'HANSOLO')
    AND (p.table_catalog IS NULL 
        OR p.table_schema IS NULL 
        OR p.table_name IS NULL 
        OR p.column_name IS NULL 
        OR p.data_type IS NULL 
        OR p.character_maximum_length IS NULL 
        OR p.numeric_precision IS NULL 
        OR p.numeric_scale IS NULL)
