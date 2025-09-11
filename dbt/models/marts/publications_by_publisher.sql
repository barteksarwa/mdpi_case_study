with base as (
    select * from {{ ref('stg_crossref') }}
)

select
    publisher,
    count(distinct doi) as total_publications,
    sum(reference_count) as total_references,
    sum(is_referenced_by_count) as total_citations
from base
group by publisher
order by total_citations desc
