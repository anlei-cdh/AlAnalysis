ADD JAR /jar/json-serde-1.3-jar-with-dependencies.jar;

CREATE EXTERNAL TABLE al (
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
LOCATION '/logs/al';

ALTER TABLE al ADD PARTITION(`day`=20161201);
ALTER TABLE al ADD PARTITION(`day`=20161202);
ALTER TABLE al ADD PARTITION(`day`=20161203);
ALTER TABLE al ADD PARTITION(`day`=20161204);
ALTER TABLE al ADD PARTITION(`day`=20161205);
ALTER TABLE al ADD PARTITION(`day`=20161206);
ALTER TABLE al ADD PARTITION(`day`=20161207);
-- ALTER TABLE al PARTITION(`day`=20161201) SET LOCATION '/logs/al/20161201/';

hdfs dfs -put /logs/al20161201.log /logs/al/day=20161201
hdfs dfs -put /logs/al20161202.log /logs/al/day=20161202
hdfs dfs -put /logs/al20161203.log /logs/al/day=20161203
hdfs dfs -put /logs/al20161204.log /logs/al/day=20161204
hdfs dfs -put /logs/al20161205.log /logs/al/day=20161205
hdfs dfs -put /logs/al20161206.log /logs/al/day=20161206
hdfs dfs -put /logs/al20161207.log /logs/al/day=20161207

CREATE EXTERNAL TABLE al_parquet (
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
LOCATION '/logs/al_parquet';

SET parquet.compression=SNAPPY;
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE al_parquet PARTITION(`day`) SELECT * FROM al;

impala-shell
invalidate metadata;