up:
	docker compose up -d

# Everything in containers, production shape. The -f is what selects it: naming a file replaces
# the default list, so docker-compose.override.yml is not merged. Drop it and this target
# quietly becomes `make dev`.
#
# kafka-init is named first so the ordering is visible here; depends_on enforces it regardless.
up-apps:
	docker compose -f docker-compose.yml --profile apps up -d --build kafka-init
	docker compose -f docker-compose.yml --profile apps up -d --build

# Dev shape: same services plus the Vite dev server on 5173 and uvicorn --reload. No -f, so the
# override is merged.
dev:
	docker compose --profile apps up -d --build

# No profile flag and no -f, so this also removes the profiled and dev-only containers.
down:
	docker compose --profile apps down

logs:
	docker compose --profile apps logs -f producer aggregator web

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