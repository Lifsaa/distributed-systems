LEADER ?= localhost:5001
REPLICAS ?= replica1=localhost:5001,replica2=localhost:5002,replica3=localhost:5003,replica4=localhost:5004,replica5=localhost:5005
LOG_DIRS := server-logs/replica1 server-logs/replica2 server-logs/replica3 server-logs/replica4 server-logs/replica5

.PHONY: up down stop logs demo seed load load20 load30 load40 test-logs test prepare-logs clean-logs reset-scenario save-scenario clean

up: prepare-logs
	docker compose up -d --build

down:
	docker compose down -v

stop:
	docker compose stop

logs:
	docker compose logs -f

demo:
	go run . client -addr $(LEADER) -op demo

seed:
	go run . client -addr $(LEADER) -op open -username seed -amount 10000

load:
	go run . client -addr $(LEADER) -op load -clients 100 -requests 1000

load20:
	go run . client -addr $(LEADER) -op load -clients 5 -requests 20

load30:
	go run . client -addr $(LEADER) -op load -clients 5 -requests 30

load40:
	go run . client -addr $(LEADER) -op load -clients 5 -requests 40

test-logs:
	go run . tester -replicas $(REPLICAS)

test:
	go test ./...

prepare-logs:
	mkdir -p $(LOG_DIRS)

clean-logs:
	rm -rf server-logs

reset-scenario:
	$(MAKE) down
	$(MAKE) clean-logs
	$(MAKE) prepare-logs

save-scenario:
	@test -n "$(NAME)" || (echo "usage: make save-scenario NAME=scenario1" && exit 1)
	mkdir -p recordings/$(NAME)
	rm -rf recordings/$(NAME)/server-logs
	cp -R server-logs recordings/$(NAME)/server-logs
	docker compose logs --no-color > recordings/$(NAME)/docker-compose.log
	docker compose ps > recordings/$(NAME)/docker-compose-ps.txt
	$(MAKE) test-logs > recordings/$(NAME)/tester-output.log 2>&1
	cat recordings/$(NAME)/tester-output.log

clean:
	rm -f bank*.db bank*.db-shm bank*.db-wal pa8.exe pa8
