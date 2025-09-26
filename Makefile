.PHONY: all fmt mod-tidy install-tools test test-coverage betteralign gofumpt code

all: mod-tidy fmt test test-coverage

fmt:
	for file in $$(find . -name "*.go"); do go fmt "$${file}"; done

mod-tidy:
	go mod tidy

install-tools:
	go install github.com/dkorunic/betteralign/cmd/betteralign@latest
	go install mvdan.cc/gofumpt@latest
	go install github.com/axw/gocov/gocov@latest
	go install github.com/matm/gocov-html/cmd/gocov-html@latest

test:
	cd _examples;go test -v

test-coverage:
	cd _examples;go test -v -coverprofile=.coverage.out -coverpkg=github.com/cd365/hey/v5,examples;gocov convert .coverage.out | gocov-html > .coverage.html

betteralign:
	betteralign -apply -fix ./...

gofumpt:
	gofumpt -w .

code: betteralign fmt gofumpt