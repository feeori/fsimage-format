-- create tmp table to load data from csv file
-- drop table if exists default.hdfsimage_file_IMAGEFILE__tmp;

CREATE EXTERNAL TABLE hdfsimage_file_IMAGEFILE__tmp ( 
 path string , 
 repl smallint , 
 mtime TIMESTAMP , 
 atime TIMESTAMP , 
 preferredblocksize INT ,
 blockcount INT, 
 filesize BIGINT , 
 nsquota INT , 
 dsquota INT , 
 permission STRING , 
 username STRING , 
 groupname STRING ,
 ppath STRING,
 mtime_step_day int,
 atime_step_day int,
 filesize_step_mb int,
 depth smallint,
 time timestamp,
 partpath string,
 partday string
 ) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/tmp/fsimage';

-- create hive table
drop table if exists default.hdfsimage_file;

--CREATE TABLE default.hdfsimage_file (
--  path STRING,
--  repl SMALLINT,
--  mtime TIMESTAMP,
--  atime TIMESTAMP,
--  preferredblocksize INT,
--  blockcount INT,
--  filesize BIGINT,
--  nsquota INT,
--  dsquota INT,
--  permission STRING,
--  username STRING,
--  groupname STRING,
--  ppath STRING,
--  mtime_step_day INT,
--  atime_step_day INT,
--  filesize_step_mb INT,
--  time TIMESTAMP
--)
--PARTITIONED BY (
--  partpath STRING,
--  partday STRING
--)
--STORED AS PARQUET;
--
--set hive.exec.dynamic.partition=true;
--set hive.exec.dynamic.partition.mode=nonstrict;
--SET hive.exec.max.dynamic.partitions=100000;
--set hive.exec.max.dynamic.partitions.pernode=100000;
--set hive.exec.max.created.files=100000;
-- load data
--INSERT INTO hdfsimage_file
--PARTITION (partpath, partday)
--SELECT
-- path, 
-- repl, 
-- mtime,
-- atime,
-- preferredblocksize,
-- blockcount, 
-- filesize, 
-- nsquota, 
-- dsquota, 
-- permission, 
-- username, 
-- groupname, 
-- ppath,
-- mtime_step_day,
-- atime_step_day,
-- filesize_step_mb,
-- time,
-- partpath,
-- partday
--FROM
--hdfsimage_file_IMAGEFILE__tmp;
--
--drop table if exists default.hdfsimage_file_IMAGEFILE__tmp;
