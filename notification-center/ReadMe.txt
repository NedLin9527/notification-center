Kafka:

defaul create
======
kafka-topics --bootstrap-server localhost:9092 --topic allowed-user-command --create --partitions 3 --replication-factor 2
kafka-topics --bootstrap-server localhost:9092 --topic allowed-user-events --create --partitions 3 --replication-factor 2
kafka-topics --bootstrap-server localhost:9092 --topic allowed-user --create --partitions 3 --replication-factor 2 --config cleanup.policy=compact
kafka-topics --bootstrap-server localhost:9092 --topic mail-command --create --partitions 3 --replication-factor 2
kafka-topics --bootstrap-server localhost:9092 --topic mail-events --create --partitions 3 --replication-factor 2 --config cleanup.policy=compact
kafka-topics --bootstrap-server localhost:9092 --topic sms-command --create --partitions 3 --replication-factor 2
kafka-topics --bootstrap-server localhost:9092 --topic sms-events --create --partitions 3 --replication-factor 2 --config cleanup.policy=compact
kafka-topics --bootstrap-server localhost:9092 --topic push-command --create --partitions 3 --replication-factor 2
kafka-topics --bootstrap-server localhost:9092 --topic push-events --create --partitions 3 --replication-factor 2 --config cleanup.policy=compact
======

Swagger UI
  URL
    http://localhost:8080/su

 ====change LOG path
 log4j2.yml :10 log.path
