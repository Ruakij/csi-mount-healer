# Cross-compiling from the build platform, so a multi-arch build needs no
# emulated toolchain.
FROM --platform=$BUILDPLATFORM golang:1.27-alpine@sha256:eb61c3873c97416fff0116553fff65d67a3549f72e11d9758849a689cdd8b523 AS build
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
# Binary needs root for mount-access on kubelets-dir aswell as setting the immutable
USER 0:0
COPY --from=build /out/csi-mount-healer /csi-mount-healer
ENTRYPOINT ["/csi-mount-healer"]
