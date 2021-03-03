drop table IF EXISTS default.fsimage_kudu;

use default;
CREATE TABLE `fsimage_kudu`(
 path string, 
 repl smallint, 
 mtime TIMESTAMP, 
 atime TIMESTAMP, 
 preferredblocksize int,
 blockcount int, 
 filesize BIGINT, 
 nsquota INT, 
 dsquota INT, 
 permission string, 
 username string, 
 groupname string,
 ppath string,
 mtime_step_day int,
 atime_step_day int,
 filesize_step_mb int,
 depth smallint,
 time timestamp,
 partpath string,
 partday string,
PRIMARY KEY (path))
PARTITION BY HASH PARTITIONS 16 STORED AS KUDU;

invalidate metadata;

insert into table fsimage_kudu select * from default.hdfsimage_file_IMAGEFILE__tmp;
