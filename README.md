# kafka-practice

start zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

Start kafka
bin/kafka-server-start.sh config/server.properties

create topic
bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --topic firsttopic --create --partitions 3 --replication-factor 1
