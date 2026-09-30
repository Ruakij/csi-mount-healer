# Cross-compiling from the build platform, so a multi-arch build needs no
# emulated toolchain.
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
ARG TARGETARCH
ARG version=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w -X main.version=${version}" -o /out/csi-mount-healer ./cmd/csi-mount-healer

# The binary is static, so the image needs nothing else: no shell or tools for
# anyone who gets code execution in this privileged container.
FROM scratch
COPY --from=build /out/csi-mount-healer /csi-mount-healer
ENTRYPOINT ["/csi-mount-healer"]
