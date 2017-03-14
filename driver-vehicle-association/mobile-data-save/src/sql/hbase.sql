
//hbase
create 'mobile','data'

//phoenix :create table with the exist hbase table

create view if not exists "mobile"
("id" VARCHAR NOT NULL PRIMARY KEY,
"data"."ts" UNSIGNED_LONG,
"data"."imsi" VARCHAR,
 "data"."imei" VARCHAR,
 "data"."mobile_cid" UNSIGNED_LONG,
 "data"."cid" UNSIGNED_LONG
 )SALT_BUCKETS=60

drop view "mobile"

 ##default_column_family = 'FM'