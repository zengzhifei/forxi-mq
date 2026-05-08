VERSION ?= $(shell git describe --tags --always --dirty)

.PHONY: build test vet frontend release clean

## build: Build all Go packages
build: frontend
	go build ./...

## vet: Run go vet
vet:
	go vet ./...

## test: Run tests
test:
	go test ./...

## frontend: Build dashboard frontend
frontend:
	cd dashboard/ui && npm install && npm run build

## release: Build, verify, and tag a release (usage: make release VERSION=v0.1.0)
release: frontend build vet
	@if [ -z "$(VERSION)" ]; then echo "Usage: make release VERSION=v0.1.0"; exit 1; fi
	@echo ""
	@echo "==> Ready to release $(VERSION)"
	@echo ""
	git add .
	git status
	@echo ""
	@read -p "Commit and tag $(VERSION)? [y/N] " confirm; \
	if [ "$$confirm" = "y" ] || [ "$$confirm" = "Y" ]; then \
		git commit -m "release: $(VERSION)"; \
		git tag $(VERSION); \
		echo "Tagged $(VERSION). Run 'make push' to publish."; \
	fi

## push: Push commits and tags to origin
push:
	git push origin main --tags

## clean: Remove build artifacts
clean:
	rm -rf dashboard/ui/node_modules dashboard/ui/dist

## help: Show this help
help:
	@grep -E '^## ' Makefile | sed 's/## //' | column -t -s ':'
