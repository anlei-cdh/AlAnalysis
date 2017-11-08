ADD JAR /jar/json-serde-1.3-jar-with-dependencies.jar;

CREATE EXTERNAL TABLE aura (
act STRING,
ts BIGINT,
ip BIGINT,
country STRING,
area STRING,
proxy BIGINT,
uuid STRING,
uid STRING,
guid STRING,
isNewUser BOOLEAN,
isNewSession BOOLEAN,
contentid BIGINT,
channelid INT,
url STRING,
title STRING,
refer STRING,
lastVisitTime BIGINT,
ua STRING,
resolution STRING,
lang STRING,
timezone INT,
flash STRING,
colordepth INT,
ctype STRING,
hasjava BOOLEAN,
keywords STRING,
searchEngine STRING
)
PARTITIONED BY (
day INT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS textfile
LOCATION '/logs/aura';

ALTER TABLE aura ADD PARTITION(`day`=20161201);
ALTER TABLE aura ADD PARTITION(`day`=20161202);
ALTER TABLE aura ADD PARTITION(`day`=20161203);
ALTER TABLE aura ADD PARTITION(`day`=20161204);
ALTER TABLE aura ADD PARTITION(`day`=20161205);
ALTER TABLE aura ADD PARTITION(`day`=20161206);
ALTER TABLE aura ADD PARTITION(`day`=20161207);
-- ALTER TABLE aura PARTITION(`day`=20161201) SET LOCATION '/logs/aura/20161201/';

hdfs dfs -put /logs/aura20161201.log /logs/aura/day=20161201
hdfs dfs -put /logs/aura20161202.log /logs/aura/day=20161202
hdfs dfs -put /logs/aura20161203.log /logs/aura/day=20161203
hdfs dfs -put /logs/aura20161204.log /logs/aura/day=20161204
hdfs dfs -put /logs/aura20161205.log /logs/aura/day=20161205
hdfs dfs -put /logs/aura20161206.log /logs/aura/day=20161206
hdfs dfs -put /logs/aura20161207.log /logs/aura/day=20161207

CREATE EXTERNAL TABLE aura_parquet (
act STRING,
ts BIGINT,
ip BIGINT,
country STRING,
area STRING,
proxy BIGINT,
uuid STRING,
uid STRING,
guid STRING,
isNewUser BOOLEAN,
isNewSession BOOLEAN,
contentid BIGINT,
channelid INT,
url STRING,
title STRING,
refer STRING,
lastVisitTime BIGINT,
ua STRING,
resolution STRING,
lang STRING,
timezone INT,
flash STRING,
colordepth INT,
ctype STRING,
hasjava BOOLEAN,
keywords STRING,
searchEngine STRING
)
PARTITIONED BY (
day INT
)
STORED AS PARQUET
LOCATION '/logs/aura_parquet';

SET parquet.compression=SNAPPY;
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE aura_parquet PARTITION(`day`) SELECT * FROM aura;

impala-shell
invalidate metadata;