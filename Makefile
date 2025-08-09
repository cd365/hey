.PHONY: all fmt mod-tidy test test-coverage install-tools code betteralign gofumpt

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
	go install mvdan.cc/gofumpt@latest
	go install github.com/axw/gocov/gocov@latest
	go install github.com/matm/gocov-html/cmd/gocov-html@latest

code: betteralign fmt gofumpt

betteralign:
	betteralign -apply -fix ./...

gofumpt:
	gofumpt -w .