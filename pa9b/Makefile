.PHONY: up down render zoom test clean logs build

up:
	docker compose up -d ray-head ray-worker-1 ray-worker-2 prometheus grafana

down:
	docker compose down

build:
	docker compose build

render:
	docker compose run --rm app

zoom:
	EXTRA_ARGS="--zoom --frames 72" docker compose run --rm app

test:
	pytest -q

clean:
	rm -rf outputs/

logs:
	docker compose logs -f ray-head ray-worker-1 ray-worker-2
