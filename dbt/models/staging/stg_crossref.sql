with source as (
    select * from crossref_data
)

select
    doi,
    title,
    publisher,
    journal,
    published_date,
    reference_count,
    is_referenced_by_count,
    -- flatten authors JSON into a string
    array_to_string(array(
        select jsonb_array_elements_text(authors::jsonb)
    ), ', ') as authors_flat
from source