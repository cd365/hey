.PHONY: code
code: betteralign fmt gofumpt

.PHONY: betteralign
betteralign:
	betteralign -apply -fix ./...
	betteralign -apply -fix ./cst/...
	betteralign -apply -fix ./status/...

.PHONY: gofumpt
gofumpt:
	gofumpt -w .
	gofumpt -w ./cst/
	gofumpt -w ./status/

.PHONY: fmt
fmt:
	for file in $$(find . -name "*.go"); do go fmt "$${file}"; done

.PHONY: all
all: mod-tidy fmt test test-coverage

.PHONY: mod-tidy
mod-tidy:
	go mod tidy

.PHONY: install
install:
	go install github.com/dkorunic/betteralign/cmd/betteralign@latest
	go install mvdan.cc/gofumpt@latest

.PHONY: run
run:
	@cd _examples;go run .

.PHONY: test
test:
	@cd _examples/all;go test -v

.PHONY: test-coverage
test-coverage:
	@cd _examples/all;go test -v -coverprofile=.coverage.out -coverpkg=github.com/cd365/hey/v6,examples/pgsql;go tool cover -html=.coverage.out -o .coverage.html
