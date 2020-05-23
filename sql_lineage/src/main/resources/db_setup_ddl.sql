-- Need to run the next only once:
-- Perhaps out-dated: CREATE USER 'sqlparser'@'%' IDENTIFIED BY 'sqlparser';
-- CREATE USER dw_app IDENTIFIED BY '';
-- GRANT ALL PRIVILEGES ON *.* TO 'dw_app';
-- flush privileges;



DROP SCHEMA IF EXISTS kachess;
CREATE SCHEMA kachess DEFAULT CHARACTER SET='utf8';
USE kachess;

create table version (
    current varchar(64) primary key
    , created_dt datetime not null
    , updated_dt timestamp
) engine=innodb
COMMENT 'This is used to compare value in VERSION file during installation. Use Flyway in the future'
;
-- INSERT IGNORE INTO version (current, created_dt) values ('1.0', now());

-- For example, a HQL file that contains multiple scripts
-- ala a DAG
CREATE TABLE sql_source (
    id int unsigned primary key
    , source_name varchar(16) not null      COMMENT '"airflow", "dashboard", etc.'
    , source_locator varchar(256) not null  COMMENT 'If airflow, DAG_ID.Task_ID. If dashboard, it is the URL'
    , source_info_extra varchar(1024) null  COMMENT 'If source has additional info, e.g. SQL file location'
    , sql_dialect varchar(32) null          COMMENT 'Hive, Spark or Presto'
    , version varchar(64) default null      COMMENT 'e.g. Git commit hash'
    , created_dt datetime not null
    , updated_dt timestamp
    , UNIQUE INDEX unique_rec_idx (source_name, source_locator)
) engine=innodb
COMMENT 'Where was the SQL first discovered/defined'
;

CREATE TABLE `dataset` (
    id int unsigned primary key
    , type varchar(16) not null                 COMMENT 'subquery, cte, table'
    , map_to_schema varchar(64) default null    COMMENT 'Not null only when a dataset is used to directly populate a table'
    , map_to_table varchar(128) default null    COMMENT 'Same as map_to_schema. Both values are set together'
    , is_aggregated boolean                     COMMENT 'Is the dataset a result of a GROUP BY'
    , start_line int default null               COMMENT 'Location coords in SQL file for this dataset'
    , start_pos int default null
    , end_line int default null
    , end_pos int default null
    , created_dt datetime not null
    , updated_dt timestamp
) engine=innodb
COMMENT 'DataSets is mostly a SELECT block. It maps to a physical Table if used in INSERT to populate it'
;

CREATE TABLE `dataset_rel` (
     parent_dataset_id int unsigned
     , child_dataset_id int unsigned
     , created_dt datetime not null
     , updated_dt timestamp
     , PRIMARY KEY (parent_dataset_id, child_dataset_id)
     , INDEX reverse_idx (child_dataset_id)
 ) engine=innodb
COMMENT 'Dependent relationship among datasets'
;

CREATE TABLE `select_item` (
    id int unsigned primary key
    , dataset_id int unsigned not null
    , name varchar(64) default null             COMMENT 'Show-up-as. Default to _col#, where # is position in list'
    , definition varchar(1024) default null     COMMENT 'Column definition. Could be "col" in "tab.col" in SELECT. Or the Function text'
    , usage_context varchar(16) not null        COMMENT 'Where is this item defined or used, e.g. "SELECT", "JOIN" and "WHERE"'
    , alias varchar(64) default null            COMMENT 'Similar to prefix, the alias in "col as a" in SELECT'
    , map_to_schema varchar(64) default null    COMMENT 'Not null only when a SelectItem refers to an existing column'
    , map_to_table varchar(128) default null    COMMENT 'Same as map_to_schema'
    , map_to_column varchar(128) default null   COMMENT 'Same as map_to_schema. All three values are set together'
    , is_simple_column boolean default NULL     COMMENT 'True only if it is a direct reference to another column. Not even complex data type'
    , data_type varchar(128) default null       COMMENT 'Hive data type. Set if we are sure what it is'
    , function_type varchar(32) default null    COMMENT 'If the SelectItem is a function, can be Window, Scalar, Aggregate'
    , extra_info varchar(1024) default null     COMMENT 'e.g. UNION with abc.col2'
    , start_line int default null               COMMENT 'Location coords in SQL file for this select item'
    , start_pos int default null
    , end_line int default null
    , end_pos int default null
    , created_dt datetime not null
    , updated_dt timestamp
    , index dataset_fk(dataset_id)
) engine=innodb
COMMENT 'SelectItem refers to the items used in a SELECT block. It can be functions, case statements, etc.'
;

CREATE TABLE `select_item_rel` (
     parent_select_item_id int unsigned
     , child_select_item_id int unsigned
     , usage_context varchar(16) not null        COMMENT 'Where is this item defined or used, e.g. "SELECT", "JOIN" and "WHERE"'
     , created_dt datetime not null
     , updated_dt timestamp
     , PRIMARY KEY (parent_select_item_id, child_select_item_id, usage_context)
     , INDEX reverse_idx (child_select_item_id)
 ) engine=innodb
COMMENT 'Dependent relationship among select_items'
;

CREATE TABLE table_insert (
    dataset_id int unsigned
    , sql_source_id int unsigned                COMMENT 'Which job caused this change? Should be there for every DDL type'
    , db_schema varchar(64) not null
    , db_table varchar(128) not null
    , type varchar(64) default null             COMMENT 'e.g. OVERWRITE, INTO - actual text after the INSERT keyword'
    , created_dt datetime not null
    , updated_dt timestamp
    , PRIMARY KEY (dataset_id, db_schema, db_table)
) engine=innodb
COMMENT 'This table captures the INSERT statements'
;

CREATE TABLE dashboard_dataset (
    dataset_id int unsigned
    , sql_source_id int unsigned                COMMENT 'Which dashboard refers this dataset?'
    , created_dt datetime not null
    , updated_dt timestamp
    , PRIMARY KEY (sql_source_id, dataset_id)
) engine=innodb
COMMENT 'This table captures which dashboard refers to which SELECT.'
;

CREATE TABLE ts_dboard_lineage (
    ts_dboard_native_id bigint not null
    , src_db_schema varchar(64) not null
    , src_db_table varchar(128) not null
    , src_db_column varchar(128) not null
    , created_dt datetime not null
    , updated_dt timestamp
) engine=innodb
    COMMENT 'This table captures all physical table columns a Tinyspeck dashboard ever used'
;

CREATE TABLE table_add_partition (
    sql_source_id int unsigned                  COMMENT 'Which job caused this change? Should be there for every DDL type'
    , db_schema varchar(64) not null
    , db_table varchar(128) not null
    , location varchar(512) default null        COMMENT 'Location that may tie tables together'
    , created_dt datetime not null
    , updated_dt timestamp
    , PRIMARY KEY (db_schema, db_table)
) engine=innodb
COMMENT 'This table captures the ALTER TABLE ADD PARTITION statements'
;

CREATE TABLE `table_symlinked` (
    src_schema varchar(64) not null
    , src_table varchar(128) not null
    , dst_schema varchar(64) not null
    , dst_table varchar(128) not null
    , linkage_src_key varchar(128) not null 	COMMENT 'Is it due to LatestOperator or Storage Linked or what else'
    , linkage_src_value text	 		        COMMENT 'More details of each type, e.g. S3 location, respectively'
    , af_dag_task varchar(128)			        COMMENT 'If not null, this symlink operation is created by a AF task'
    , created_dt datetime not null
    , updated_dt timestamp
    , PRIMARY KEY (src_schema, src_table, dst_schema, dst_table)
    , INDEX reverse_idx (dst_schema, dst_table)
) engine=innodb
COMMENT 'LT is akin to symlink files. The src table is always the one found during parsing. And the dst tables are found outside parsing such as by LatestOperator or Storage-Linked'
;

-- The following table are "summary tables" in that they may be populated later
CREATE TABLE table_lineage (
    child_schema varchar(64) not null
    , child_table varchar(128) not null
    , parent_schema varchar(64) not null
    , parent_table varchar(128) not null
    , created_dt datetime not null
    , updated_dt timestamp
    , PRIMARY KEY (child_schema, child_table, parent_schema, parent_table)
    , INDEX revserse_idx (parent_schema, parent_table)
) engine=innodb
COMMENT 'Dependency edges among physical/persisted tables that can be discovered through SQL parsing'
;

CREATE TABLE column_lineage (
    child_schema varchar(64) not null
    , child_table varchar(128) not null
    , child_column varchar(128) not null
    , parent_schema varchar(64) not null
    , parent_table varchar(128) not null
    , parent_column varchar(128) not null
    , distance int default null
    , created_dt datetime not null
    , updated_dt timestamp
    , PRIMARY KEY (child_schema, child_table, child_column, parent_schema, parent_table, parent_column)
    , INDEX revserse_idx (parent_schema, parent_table, parent_column)
) engine=innodb;

CREATE TABLE sqoop_table (
    db_schema varchar(64) not null
    , db_table varchar(128) not null
    , etl_task_name varchar(128) not null
    , created_dt datetime not null
    , updated_dt timestamp
    , PRIMARY KEY (db_schema, db_table, etl_task_name)
) engine=innodb
;

CREATE TABLE ts_dboard (
    native_id bigint not null
    , title varchar(512) default null
    , native_created_ts bigint not null
    , native_updated_ts bigint not null
    , author varchar(128) default null
    , created_dt datetime not null
    , updated_dt timestamp
    , PRIMARY KEY (native_id)
) engine=innodb
    COMMENT 'TS_DBOARD stands for TinySpeck DashBOARD'
;
