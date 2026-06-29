URLS  ?= 10000
WORKERS ?= 4
BATCH ?= 100

build:
	GOOS=linux go build -o search-engine .

image:
	docker compose build

up: build
	MAX_URLS=$(URLS) BATCH_SIZE=$(BATCH) docker compose up --build --scale worker=$(WORKERS)

run:
	MAX_URLS=$(URLS) BATCH_SIZE=$(BATCH) docker compose up --scale worker=$(WORKERS)

down:
	docker compose down

clean:
	rm -f search-engine mr-out-*.json intermediate-*.json
	rm -rf mr-output/* coord-logs/*

s1a: build
	MAX_URLS=100   BATCH_SIZE=100 docker compose up --build --scale worker=4
fs1a:
	MAX_URLS=100   BATCH_SIZE=100 docker compose up --scale worker=4
s1b: build
	MAX_URLS=1000  BATCH_SIZE=100 docker compose up --build --scale worker=4
fs1b:
	MAX_URLS=1000  BATCH_SIZE=100 docker compose up --scale worker=4
s1c: build
	MAX_URLS=5000  BATCH_SIZE=100 docker compose up --build --scale worker=4
fs1c:
	MAX_URLS=5000  BATCH_SIZE=100 docker compose up --scale worker=4
s1d: build
	MAX_URLS=10000 BATCH_SIZE=100 docker compose up --build --scale worker=4
fs1d:
	MAX_URLS=10000 BATCH_SIZE=100 docker compose up --scale worker=4
s1e: build
	MAX_URLS=20000 BATCH_SIZE=100 docker compose up --build --scale worker=4
fs1e:
	MAX_URLS=20000 BATCH_SIZE=100 docker compose up --scale worker=4

s2a: build
	MAX_URLS=10000 BATCH_SIZE=100 docker compose up --build --scale worker=2
fs2a:
	MAX_URLS=10000 BATCH_SIZE=100 docker compose up --scale worker=2
s2b: build
	MAX_URLS=10000 BATCH_SIZE=100 docker compose up --build --scale worker=3
fs2b:
	MAX_URLS=10000 BATCH_SIZE=100 docker compose up --scale worker=3
s2c: build
	MAX_URLS=10000 BATCH_SIZE=100 docker compose up --build --scale worker=4
fs2c:
	MAX_URLS=10000 BATCH_SIZE=100 docker compose up --scale worker=4

s3a: build
	MAX_URLS=10000 BATCH_SIZE=100 docker compose up --build --scale worker=4
fs3a:
	MAX_URLS=10000 BATCH_SIZE=100 docker compose up --scale worker=4
s3b: build
	MAX_URLS=10000 BATCH_SIZE=200 docker compose up --build --scale worker=4
fs3b:
	MAX_URLS=10000 BATCH_SIZE=200 docker compose up --scale worker=4
s3c: build
	MAX_URLS=10000 BATCH_SIZE=500 docker compose up --build --scale worker=4
fs3c:
	MAX_URLS=10000 BATCH_SIZE=500 docker compose up --scale worker=4
