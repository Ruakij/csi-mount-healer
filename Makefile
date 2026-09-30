VERSION ?= dev
LDFLAGS = -s -w -X main.version=$(VERSION)

.PHONY: build
build:
	CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "$(LDFLAGS)" -o bin/csi-mount-healer ./cmd/csi-mount-healer

# Mount tests need root and a Linux kernel, so they run in a privileged container.
# On a non-Linux machine that container is inside colima or another VM.
MOUNTTEST = docker run --rm --privileged -v $(CURDIR):/src -w /src -e GOFLAGS=-buildvcs=false \
	golang:1.27-alpine go test -tags mounttest

.PHONY: test-mount
test-mount:
	$(MOUNTTEST) -count=1 -v ./...
