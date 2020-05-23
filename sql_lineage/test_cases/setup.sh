#!/bin/bash

# Truncate db:
mysql -AB -u root --skip-column-names << _END > /tmp/run_truncdb.sql
select concat('truncate table ', TABLE_SCHEMA, '.', TABLE_NAME, ';') from information_schema.tables where
TABLE_SCHEMA='bingql';
_END

mysql -AB -u root --skip-column-names < /tmp/run_truncdb.sql

mysql -AB -u root bingql << _END
-- Set up init tables:
use bingql;

insert into dataset (id , type , map_to_schema , map_to_table , created_dt) 
values 
  ( 10 ,  'TABLE' , 'dw' , 'tab10', now())
, ( 20 ,  'TABLE' , 'dw' , 'tab20' , now())
, ( 30 ,  'TABLE' , 'stage' , 'tab30' , now())
, ( 40 ,  'TABLE' , 'adhoc' , 'tab40' , now())
;

insert ignore into select_item 
(dataset_id, id ,name ,definition ,usage_context ,is_simple_column ,map_to_schema ,map_to_table ,map_to_column ,data_type ,created_dt) 
values 
  ( 10, 100, 'col100l' , 'col100l' , 'SELECT' , 1 , 'dw' , 'tab10' , 'col100l' , 'bigint' , now())
, ( 10, 101, 'col101s' , 'col101s' , 'SELECT' , 1 , 'dw' , 'tab10' , 'col101s' , 'string' , now())
, ( 10, 102, 'col102b' , 'col102b' , 'SELECT' , 1 , 'dw' , 'tab10' , 'col102b' , 'boolean' , now())
, ( 10, 103, 'col103i' , 'col103i' , 'SELECT' , 1 , 'dw' , 'tab10' , 'col103i' , 'int' , now())
, ( 20, 200, 'col200l' , 'col200l' , 'SELECT' , 1 , 'dw' , 'tab20' , 'col200l' , 'bigint' , now())
, ( 20, 201, 'col201s' , 'col201s' , 'SELECT' , 1 , 'dw' , 'tab20' , 'col201s' , 'string' , now())
, ( 20, 202, 'col202b' , 'col202b' , 'SELECT' , 1 , 'dw' , 'tab20' , 'col202b' , 'boolean' , now())
, ( 20, 203, 'col203i' , 'col203i' , 'SELECT' , 1 , 'dw' , 'tab20' , 'col203i' , 'int' , now())
, ( 30, 300, 'col300l' , 'col300l' , 'SELECT' , 1 , 'stage' , 'tab30' , 'col300l' , 'bigint' , now())
, ( 30, 301, 'col301s' , 'col301s' , 'SELECT' , 1 , 'stage' , 'tab30' , 'col301s' , 'string' , now())
, ( 30, 302, 'col302b' , 'col302b' , 'SELECT' , 1 , 'stage' , 'tab30' , 'col302b' , 'boolean' , now())
, ( 30, 303, 'col303i' , 'col303i' , 'SELECT' , 1 , 'stage' , 'tab30' , 'col303i' , 'int' , now())
, ( 40, 400, 'col400l' , 'col400l' , 'SELECT' , 1 , 'adhoc' , 'tab40' , 'col400l' , 'bigint' , now())
, ( 40, 401, 'col401s' , 'col401s' , 'SELECT' , 1 , 'adhoc' , 'tab40' , 'col401s' , 'string' , now())
, ( 40, 402, 'col402x' , 'col402x' , 'SELECT' , 1 , 'adhoc' , 'tab40' , 'col402x' , 'array<struct<user:struct<id:bigint>,group:struct<id:bigint>,is_bot:boolean>>', now())
, ( 40, 403, 'col403i' , 'col403i' , 'SELECT' , 1 , 'adhoc' , 'tab40' , 'col403i' , 'int' , now())
;

_END
