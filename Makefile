up:
	docker compose up -d

down:
	docker compose down

topic:
	docker exec kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create --if-not-exists \
		--topic market-events \
		--partitions 3 \
		--replication-factor 1

topics:
	docker exec kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 --list