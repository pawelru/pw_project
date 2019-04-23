kafka/bin/zookeeper-server-start.sh kafka/config/zookeeper.properties > zookeeper.log 2>&1 &
kafka/bin/kafka-server-start.sh kafka/config/server.properties > kafka.log 2>&1 &
kafka/bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic posts_all
kafka/bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic posts_clean
kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 6 --topic posts_all
kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 6 --topic posts_clean
./kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
