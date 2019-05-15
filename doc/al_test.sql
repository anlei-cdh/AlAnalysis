TRUNCATE `hive_dimension_data`;

TRUNCATE `storm_dimension_data`;
TRUNCATE `storm_content_data`;
TRUNCATE `storm_content_detail`;

TRUNCATE `streaming_dimension_data`;
TRUNCATE `streaming_content_data`;
TRUNCATE `streaming_content_detail`;

TRUNCATE `sparkcore_dimension_data`;
TRUNCATE `sparkcore_content_data`;
TRUNCATE `sparkcore_content_detail`;

TRUNCATE `mllib_gender_data`;
TRUNCATE `mllib_channel_data`;

TRUNCATE `ml_lr_data`;
TRUNCATE `ml_dt_data`;
TRUNCATE `ml_clustering_data`;
TRUNCATE `ml_cf_user_data`;
TRUNCATE `ml_cf_item_data`;

SELECT * FROM `hive_dimension_data`;

SELECT * FROM `storm_dimension_data`;
SELECT * FROM `storm_content_data`;
SELECT * FROM `storm_content_detail`;

SELECT * FROM `streaming_dimension_data`;
SELECT * FROM `streaming_content_data`;
SELECT * FROM `streaming_content_detail`;

SELECT * FROM `sparkcore_dimension_data`;
SELECT * FROM `sparkcore_content_data`;
SELECT * FROM `sparkcore_content_detail`;

SELECT * FROM `mllib_gender_data`;
SELECT * FROM `mllib_channel_data`;

SELECT * FROM `ml_lr_data`;
SELECT * FROM `ml_dt_data`;
SELECT * FROM `ml_clustering_data`;
SELECT * FROM `ml_cf_user_data`;
SELECT * FROM `ml_cf_item_data`;

================================================================================

#Kafka
-kafka创建topic
/opt/cloudera/parcels/KAFKA/bin/kafka-topics --create --zookeeper cdh01:2181,cdh02:2181,cdh03:2181 --replication-factor 1 --partitions 1 --topic al
-kafka查看topic
/opt/cloudera/parcels/KAFKA/bin/kafka-topics --list --zookeeper cdh01:2181,cdh02:2181,cdh03:2181
-kafka创建consumer
/opt/cloudera/parcels/KAFKA/bin/kafka-console-consumer --zookeeper cdh01:2181,cdh02:2181,cdh03:2181 --topic al --from-beginning

================================================================================

#OLAP
hdfs dfs -rm -r /logs/al
hdfs dfs -rm -r /logs/al_parquet
drop table al;
drop table al_parquet;
invalidate metadata;