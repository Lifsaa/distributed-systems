build:
	GOOS=linux go build -o mapreduce .

up: build
	caffeinate docker compose up --build --scale worker=3

down:
	docker compose down

output:
	cat mr-out-0 mr-out-1 mr-out-2 | sort

clean:
	rm -f mapreduce split-* intermediate-* mr-out-*
