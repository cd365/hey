.PHONY: code
code: betteralign fmt gofumpt

.PHONY: betteralign
betteralign:
	betteralign -apply -fix ./...

.PHONY: gofumpt
gofumpt:
	gofumpt -w .

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
	go install github.com/axw/gocov/gocov@latest
	go install github.com/matm/gocov-html/cmd/gocov-html@latest

.PHONY: test
test:
	cd _examples;go test -v

.PHONY: test-coverage
test-coverage:
	cd _examples;go test -v -coverprofile=.coverage.out -coverpkg=github.com/cd365/hey/v6,examples;gocov convert .coverage.out | gocov-html > .coverage.html
