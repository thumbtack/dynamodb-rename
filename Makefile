SRC = $(wildcard *.go)

fmt: $(SRC)
	$(foreach f, $?, go fmt $(f);)

test: $(SRC)
	go test -coverprofile=coverage.out

.PHONY: coverage
coverage: test
	go tool cover -html=coverage.out
