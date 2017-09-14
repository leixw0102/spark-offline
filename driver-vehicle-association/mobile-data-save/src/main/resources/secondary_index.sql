drop index if exists mobile_index on "mobile"
create index mobile_index on "mobile"("data"."ts","data"."imsi","data"."cid","data"."mobile_cid","data"."imei") SALT_BUCKETS=60