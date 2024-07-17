
#run:
#	go run app/*.go

run-etl:
	go run app/main.go

run-system: cleanup compose-up

compose-up:
	docker compose up --build

cleanup:
	rm fs/volumes/clickhouse/errors/* || true
