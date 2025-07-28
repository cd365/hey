.PHONY: all fmt mod-tidy test

all: mod-tidy fmt test

fmt:
	for file in $$(find . -name "*.go"); do go fmt "$${file}"; done

mod-tidy:
	go mod tidy

test:
	go test -v