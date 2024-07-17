start-etl: cleanup compose-up

start-etl-container-only:
	docker compose up etl --build

compose-up:
	docker compose up --build

cleanup:
	rm fs/volumes/clickhouse/errors/* || true
