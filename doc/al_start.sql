#MySQL
UPDATE `hive_dimension_data` SET pv = 0,uv = 0,ip = 0 WHERE DAY = '2016-12-07';
UPDATE `hive_dimension_data` SET time = 0 WHERE DAY = '2016-12-07';

TRUNCATE `storm_dimension_data`;
TRUNCATE `storm_content_data`;
TRUNCATE `storm_content_detail`;
TRUNCATE `dl_classify_data`;
TRUNCATE `dl_detection_data`;

DELETE FROM `dl_poetize_data` WHERE id = 3;

#Kudu
--cdh01
impala-shell
update al_kudu set count = 29,rate = 3 where id = 2;

================================================================================

#Cloudera Manager
--cdh01
sh /start-cm-server.sh
sh /start-cm-agent.sh
--cdh02
sh /start-cm-agent.sh
--cdh03
sh /start-cm-agent.sh

#BlockChain
--cdh03
sh /start-petshop.sh

#Storm
--cdh01
flume-ng avro-client -H cdh01 -p 9999 -F /logs/al.log

#Kudu
--cdh01
impala-shell
select * from al_parquet limit 2;
update al_parquet set url = 'www.baidu.com';

update al_kudu set count = 40,rate = 10 where id = 2;
show create table al_kudu;
