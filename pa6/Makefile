ADDR ?= :5001
DB ?= bank.db

server:
	go run . server -addr $(ADDR) -db $(DB)

demo:
	go run . client -op demo

load:
	go run . client -op load -clients 100 -requests 1000

log:
	sqlite3 $(DB) "SELECT * FROM operation_log LIMIT 20;"

clean:
	rm -f bank.db bank.db-shm bank.db-wal pa6.exe pa6
