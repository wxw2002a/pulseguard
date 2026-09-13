.PHONY: up down demo e2e test integration monitoring logs config
up:
	docker compose up -d --build
down:
	docker compose down
demo:
	python scripts/load_generator.py --scenario mixed --count 120 --rate 10
e2e:
	python scripts/e2e.py
test:
	./mvnw -B -ntp verify
integration:
	./mvnw -B -ntp -Pintegration,spark-tests verify
monitoring:
	docker compose --profile monitoring up -d
logs:
	docker compose logs -f api streaming
config:
	python scripts/validate_configs.py
