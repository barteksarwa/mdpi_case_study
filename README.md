
## Overview
This project builds ETL pipeline for CrossRef metadata:
1. **Extract** metadata from the CrossRef API.
2. **Transform**
   - Normalize JSON into a flat schema.
   - Deduplicate records by DOI.
3. **Load** into PostgreSQL.
4. **Analytics with dbt**
   - Publications & citations by year.
   - Publisher-level citation summaries.

The whole stack runs in **Docker** 

git clone https://github.com/barteksarwa/mdpi_case.git