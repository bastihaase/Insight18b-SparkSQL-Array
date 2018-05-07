#######################################################
# This Is The Script To Create A Topic named meta In Kafka
#######################################################


/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 2 --topic met
