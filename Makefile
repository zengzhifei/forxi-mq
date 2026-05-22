# Auto-increment patch version from latest tag
AUTO_VERSION := $(shell git describe --tags --abbrev=0 2>/dev/null | sed 's/^v//' | awk -F. '{printf "v%s.%s.%d", $$1, $$2, $$3+1}' || echo "v0.0.1")

# Capture positional argument (the word after the target)
ARG := $(filter-out push release clean help build vet test frontend,$(MAKECMDGOALS))
VERSION = $(or $(ARG),$(AUTO_VERSION))
MSG =

# Catch-all to prevent "No rule to make target" for positional args
%::
	@true

.PHONY: build test vet frontend push release clean help

build: frontend
	go build ./...

vet:
	go vet ./...

test:
	go test ./...

frontend:
	cd dashboard/ui && npm install && npm run build

## push: Commit and push code (usage: make push 'message')
push:
	@if [ -z "$(ARG)" ]; then echo "Usage: make push 'commit message'"; exit 1; fi
	git add .
	git commit -m "$(ARG)"
	git push origin main

## release: Build, bump version, tag, push (usage: make release [version] [MSG='message'])
release: frontend build vet
	@echo "==> Releasing $(VERSION)"
	git add .
	git commit -m "$(or $(MSG),release: $(VERSION))"
	@if git rev-parse "$(VERSION)" >/dev/null 2>&1; then \
		echo "==> Tag $(VERSION) exists, replacing..."; \
		git tag -d "$(VERSION)"; \
		git push origin ":refs/tags/$(VERSION)" 2>/dev/null || true; \
	fi
	git tag $(VERSION)
	git push origin main
	git push origin $(VERSION)
	@echo "==> Released $(VERSION)"

## clean: Remove build artifacts
clean:
	rm -rf dashboard/ui/node_modules dashboard/ui/dist

## help: Show this help
help:
	@grep -E '^## ' Makefile | sed 's/## //' | column -t -s ':'
