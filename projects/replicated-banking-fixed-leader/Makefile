LEADER ?= localhost:5001

up:
	docker compose up --build

down:
	docker compose down -v

demo:
	go run . client -addr $(LEADER) -op demo

load:
	go run . client -addr $(LEADER) -op load -clients 100 -requests 1000

test-logs:
	go run . tester -replicas replica1=localhost:5001,replica2=localhost:5002,replica3=localhost:5003

test:
	go test ./...

clean:
	rm -f bank*.db bank*.db-shm bank*.db-wal pa7.exe pa7
