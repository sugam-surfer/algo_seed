{{
    config(
        materialized    = 'incremental',
        unique_key      = 'id',
        cluster_by      = 'created_at::date',
      	post_hook       = ["delete from ahsokatano_4_raw.check_hansolo_trivia_transactions where dest_table_type = 'error'"],
        tags            = ['on_chain', 'trivia', 'error', 'warehouse'],
    )
}}

select * exclude (dest_table_type) from {{ ref('check_hansolo_trivia_transactions') }} where dest_table_type = 'error'

