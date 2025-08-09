.PHONY: all fmt mod-tidy test test-coverage install-tools code betteralign golangci-lint staticcheck gofumpt

all: mod-tidy fmt test

fmt:
	for file in $$(find . -name "*.go"); do go fmt "$${file}"; done

mod-tidy:
	go mod tidy

test:
	go test -v

test-coverage:
	gocov test ./... | gocov-html > .coverage.html

install-tools:
	go install github.com/dkorunic/betteralign/cmd/betteralign@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install honnef.co/go/tools/cmd/staticcheck@latest
	go install mvdan.cc/gofumpt@latest
	go install github.com/axw/gocov/gocov@latest
	go install github.com/matm/gocov-html/cmd/gocov-html@latest

code: betteralign golangci-lint staticcheck gofumpt

betteralign:
	betteralign -apply -fix ./...

golangci-lint:
	golangci-lint run --fix

staticcheck:
	staticcheck ./...

gofumpt:
	gofumpt -w .