with base as (
    select * from {{ ref('stg_crossref') }}
)

select
    extract(year from published_date) as pub_year,
    count(distinct doi) as publications,
    sum(is_referenced_by_count) as total_citations
from base
where published_date is not null
group by pub_year
order by pub_year
