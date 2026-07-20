BINARY := faster

.PHONY: build run test vet fmt clean install

build:
	go build -o $(BINARY) .

run: build
	./$(BINARY)

vet:
	go vet ./...

fmt:
	gofmt -w .

test:
	go test ./...

install:
	go install .

clean:
	rm -f $(BINARY)
