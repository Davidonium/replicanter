

.PHONY: build
build:
	go build -o build/replicanter cmd/main.go

.PHONY: run
run: build
	./build/replicanter

.PHONY: test
test:
	go test ./...

.PHONY: get
get:
	go get ./...

