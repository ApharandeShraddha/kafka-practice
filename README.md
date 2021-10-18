# kafka-practice

Project overview - 
Implememtation of Kafka producer which is taking data from Twitter i.e tweets  in real time and insert them into Kafka topic. Implememtation of kafka consumer which takes data from Kafka and puts it into Elasticsearch.

How to run Project:

start zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

Start kafka
bin/kafka-server-start.sh config/server.properties

create topic
bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --topic firsttopic --create --partitions 3 --replication-factor 1
