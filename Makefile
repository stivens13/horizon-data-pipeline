
run:
	go run app/*.go

run-all:
	rm fs/errors/* || true
	docker compose up --build