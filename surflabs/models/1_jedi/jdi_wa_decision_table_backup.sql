
{{
    config(
        materialized    = 'incremental',
        unique_key      = 'record_hash',
        tags            = ['withdrawal_automation', 'warehouse'],
    )
}}


    SELECT
        *,
        md5(
            concat_ws(
                '|',
                {% for column in adapter.get_columns_in_relation(ref('jdi_wa_decision_table')) %}
                    -- Ensure each column value is cast to string and nulls are handled
                    COALESCE(CAST({{ column.name }} AS STRING), 'NULL')
                    {% if not loop.last %} , {% endif %}
                {% endfor %}
            )
        ) AS record_hash
    FROM {{ ref('jdi_wa_decision_table') }}

    
