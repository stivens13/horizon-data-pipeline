
#run:
#	go run app/*.go


run: cleanup compose-up

compose-up:
	docker compose up --build

cleanup:
	rm fs/volumes/clickhouse/errors/* || true
