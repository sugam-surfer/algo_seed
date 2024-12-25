{{
    config(
        materialized    = 'table',
        tags            = ['schema', 'warehouse'],
    )
}}

SELECT 
    ts AS timestamp,
    c.table_catalog,
    c.table_schema,
    c.table_name, 
    c.column_name, 
    c.data_type, 
    COALESCE(CAST(c.character_maximum_length AS VARCHAR), 'NA1') AS character_maximum_length, 
    COALESCE(CAST(c.numeric_precision AS VARCHAR), 'NA2') AS numeric_precision, 
    COALESCE(CAST(c.numeric_scale AS VARCHAR), 'NA3') AS numeric_scale
FROM 
    INFORMATION_SCHEMA.COLUMNS c
LEFT JOIN 
(
    SELECT 
        COALESCE(MAX(change_time), CURRENT_TIMESTAMP) AS ts 
    FROM 
        {{ ref('xyz_schema_change_log') }}
) log 
ON TRUE
WHERE 
    c.table_schema IN ('PADME', 'HANSOLO')
