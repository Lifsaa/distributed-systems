build:
	GOOS=linux go build -o pa4 .

up: build
	caffeinate docker compose up --build --scale worker=5

down:
	docker compose down

clean:
	rm -f pa4 mr-out-*.json intermediate-*.json
