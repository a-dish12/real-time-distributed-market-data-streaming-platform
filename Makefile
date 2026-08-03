up:
	docker compose up -d

down:
	docker compose down

events-topic:
	docker exec kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create --if-not-exists \
		--topic market-events \
		--partitions 3 \
		--replication-factor 1

bars-topic:
	docker exec kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create --if-not-exists \
		--topic bars \
		--partitions 1 \
		--replication-factor 1

topics-create: events-topic bars-topic

topics:
	docker exec kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 --list

describe:
	docker exec kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 --describe