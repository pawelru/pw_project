kafka/bin/zookeeper-server-start.sh kafka/config/zookeeper.properties > zookeeper.log 2>&1 &
kafka/bin/kafka-server-start.sh kafka/config/server.properties > kafka.log 2>&1 &
