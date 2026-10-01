# csi-mount-healer Helm repository

This branch holds nothing but the Helm chart index, which the release workflow
updates on every stable release; the source lives on `main`.

```sh
helm repo add csi-mount-healer https://ruakij.github.io/csi-mount-healer
helm repo update
helm install csi-mount-healer csi-mount-healer/csi-mount-healer -n kube-system
```
