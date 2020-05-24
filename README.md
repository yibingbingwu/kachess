
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
  
<details>  
    <summary>To elaborate a bit more ... </summary>  
  
### Background  
  
- Project Kachess is inspired by my recent work at a popular software company where there was a lot of data users and SQL writers, but there was very few data engineers  
  - Many people built SQL pipelines besides data engineers: data scientists, BI developers, software engineers, product managers, etc.  
  - Most of them wrote pipelines to address their immediate needs, with a hope that one day, someday, a data engineer will take over   
- As a result, the pipelines were haphazardly done:  
  - There was a lack of rigor to ensure data fidelity, e.g. whether a pipeline uses the source of truth, whether there is data quality checks, etc.  
  - There was no enforcement of engineering best practices, e.g. adopting naming convention, breaking down complicated pipelines into smaller, re-useable components  
  - There was very little documentation for most pipelines  
- The problem tends to feed on itself  
  - Even when there are opportunities to refactor an existing table, because people are afraid of downstream impacts, they often choose the only option they are left with: to build a parallel table.  
  - After a while, when there are so many look-alike tables, one lost track of which one is the latest, or is the source of truth  
  - Even when a data engineer is ready to consolidate the pipelines, she found her scope just multiplied  
  - The cycle continues until no one trusts the data warehouse anymore  
- Similarly, there are other critical needs that could benefit greatly from data lineage, for example  
  - When a pipeline had an incident, the on-call engineer needs to find and notify downstream owners of the outage. She needs to be able to answer questions like, "how soon my dashboard will be back online"? 
  - When you decided to move your data warehouse to a new platform, which pipeline to migrate first?
  - When you have to make a data model change, which downstream pipeline needs to be re-written?
  - When InfoSec or SOX compliance teams ask for access control audit  
</details>    
  
## Why SQL parser  
  
**To discover data lineage, there is no escape of parsing SQL scripts.**
<details>
    <summary>Here is why ... </summary>  

- In a smaller setting, you may be able to use `grep`, `find` or an IDE to track downstream dependencies of a table
- However, in an large dev environment, when you have to do the same lookup repeatedly, among hundreds of pipelines or thousands of tables, the task quickly becomes productivity drain
- What is more, if all the SQL programs follow a coding standard, or conforming to a naming convention, table lookups may be much easier. 
- However, when you have a legacy code base that is several years old, and you had many turnovers on the team, you may not assume anything
</details>

So the question really becomes how do you parse SQL?
- Here are a few alternatives
    - [HiveParser](https://hive.apache.org/javadocs/r2.1.1/api/org/apache/hadoop/hive/ql/parse/HiveParser.html)
    - Uber Engineering's [QueryParser](https://github.com/uber/queryparser)
    - A few commercial options such as [MANTA](https://getmanta.com/), [General SQL Parser](http://www.sqlparser.com/)
- I chose to write my own, starting from BNF syntax description, for the following reasons:
    <details>
    <summary>- Flexibility</summary>
        - Because we used Hive to create/populate tables but Presto to query them, I need to parse both SQL dialects
        - In the future, we may need to support other analytical engines such as Redshift or Snowflake
    </details>
    - Familiarity
        - I happen to be an SQL expert who likes to write parsers
            - I have worked with many flavors of SQL in different settings. Suffice to say there is not a whole lot surprise left when it comes to SQL
            - I have written two previous machine language parsers. I can find my way around in this field
        - Although it is challenging to write one syntax that supports multiple SQL dialects, I know this is achievable because:
            - I am only to extract lineage info, not to build an execution engine
                - In other words, all I need to parse out from `INSERT INTO table_a AS SELECT * FROM table_b` is that `table_a` is a child of (or downstream from) `table_b`, not to orchestrate a Map/Reduce job
                - Nor do I need to understand the plethora of platform-specific function (e.g. `date_add` vs. `adddate`): so long I can recognize something is a function, I can grab the parameters and map them to known columns
            - At the end of the day, achieving a 90% lineage coverage of more than one SQL dialects in our code base is far more valuable then a 99.9% coverage of just one

## Where does the project stand now
## How can I try it out  
## Future development