# Project Kachess

## What is Project Kachess

Project Kachess is a data lineage tool. It collects and stores column-level dependency information by parsing SQL scripts. 
The results are stored in a MySQL database. They can be used to power data-driven applications, and to enable advanced dependency analysis.


## What are the problems Project Kachess trying to address

Project Kachess is designed for Data Engineers working in a Data Warehouse like environment, where:
- Most tables are created from common source tables by transforming pipelines
- There is a lack of convention of how the tables are generated or used
- There is a constant need to refactor the foundational tables
- There are business needs (e.g. regulatory, or contractual commitments) to discover data dependencies and/or to enforce access control

To elaborate a big more:
- Project Kachess is inspired by my recent work at a popular software company where there was a lot of data users and SQL developers, but there was very few data engineers
    - Many people wrote SQL pipelines besides data engineers: data scientists, BI developers, software engineers, product managers, etc.
    - Most of them wrote pipelines to address their immediate needs, with a hope that one day, someday, a data engineer will take them over 
- As a result, the pipelines were haphazardly done:
    - There was a lack of rigor in enforcing data fidelity, e.g. whether it uses the source of truth, whether there is data quality checks, etc.
    - There was no enforcement of engineering best practices, e.g. to develop and to follow a naming convention, to breakdown complicated pipelines into smaller, re-useable components
    - There was very little documentation for most pipelines
- The problem tends to feed on itself
    - Even when there are opportunities to refactor a table, because people are afraid of downstream impacts, they often choose the only option they are left with: to build a parallel table.
    - After a while, when there are so many look-alike tables, one lost track of which one is the latest, or is the source of truth
    - Even when a data engineer is ready to consolidate the pipelines, she found her scope just multiplied
    - The cycle continues until no one trusts the data warehouse anymore
- Similarly, there are other critical business needs are impacted by the chaos, for example
    - When you decided to move your data warehouse to a new platform, or when you have to make a data model change
    - When InfoSec team ask for access control audit
  

## Why SQL parser

***To discover data lineage, there is no escape of parsing SQL scripts.***
## How can I try it out
## Future development
