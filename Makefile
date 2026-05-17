MAELSTROM := ../maelstrom/maelstrom
BIN       := target/debug

.PHONY: build echo broadcast broadcast-partition gset gset-partition pncounter txn txn-multi

build:
	cargo build

echo: build
	$(MAELSTROM) test -w echo --bin $(BIN)/echo_server \
	  --nodes n1 --time-limit 5 --log-stderr

broadcast: build
	$(MAELSTROM) test -w broadcast --bin $(BIN)/broadcast \
	  --time-limit 5 --log-stderr

broadcast-partition: build
	$(MAELSTROM) test -w broadcast --bin $(BIN)/broadcast \
	  --time-limit 20 --nemesis partition

gset: build
	$(MAELSTROM) test -w g-set --bin $(BIN)/gset --time-limit 10

gset-partition: build
	$(MAELSTROM) test -w g-set --bin $(BIN)/gset \
	  --time-limit 30 --rate 10 --nemesis partition

pncounter: build
	$(MAELSTROM) test -w pn-counter --bin $(BIN)/pn_counter \
	  --time-limit 20 --rate 10

txn: build
	$(MAELSTROM) test -w txn-list-append --bin $(BIN)/datomic \
	  --time-limit 10 --node-count 1

txn-multi: build
	$(MAELSTROM) test -w txn-list-append --bin $(BIN)/datomic \
	  --time-limit 10 --node-count 2
