.PHONY: all fmt mod-tidy

all: fmt mod-tidy

fmt:
	for file in $$(find . -name "*.go"); do go fmt "$${file}"; done

mod-tidy:
	go mod tidy