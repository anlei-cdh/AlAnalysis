CREATE TABLE al_kudu (
  id INT,
  name STRING,
  count INT,
  rate INT
)
DISTRIBUTE BY HASH INTO 4 BUCKETS
TBLPROPERTIES(
  'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler',
  'kudu.table_name' = 'al_kudu',
  'kudu.master_addresses' = 'cdh01',
  'kudu.key_columns' = 'id'
);
INSERT INTO al_kudu VALUES(1,'新浪网',32,1);
INSERT INTO al_kudu VALUES(2,'腾讯网',29,3);
INSERT INTO al_kudu VALUES(3,'搜狐网',25,5);
INSERT INTO al_kudu VALUES(4,'网易网',21,7);
INSERT INTO al_kudu VALUES(5,'凤凰网',19,9);