# Fix: transferia Docker + kafka-go compatibility for downstream modules

## Problem

`transferia@v0.0.6-rc0` has two compile-time incompatibilities that only manifest in **downstream modules** (like `transferia/iceberg`), not in the transferia repo itself. This is because transferia uses `replace` directives in its own `go.mod` that are **non-transitive** — they only apply when building transferia directly, not when imported as a dependency.

## Issue 1: Docker client `ImagePullOptions` removed

**transferia's go.mod:**
```
github.com/docker/docker v28.5.1+incompatible
replace github.com/docker/docker => github.com/docker/docker v25.0.6+incompatible
```

transferia declares Docker v28 but replaces it with v25 locally. Downstream modules resolve Docker v28 (where `types.ImagePullOptions` was removed/moved to `moby/moby`).

**Affected files in `transferia/pkg/container`:**

| File | Line | Usage |
|---|---|---|
| `client.go` | 17 | `ImagePull(ctx, ref, docker_types.ImagePullOptions)` in interface |
| `container.go` | 15 | `Pull(context.Context, string, docker_types.ImagePullOptions)` in interface |
| `docker.go` | 60 | `func (d *DockerWrapper) Pull(ctx, image, opts docker_types.ImagePullOptions)` |
| `docker.go` | 63 | `d.cli.ImagePull(ctx, image, docker_types.ImagePullOptions{})` |
| `docker.go` | 89 | `d.Pull(ctx, opts.Image, docker_types.ImagePullOptions{})` |
| `docker_mocks.go` | 23 | `func (m *MockDockerClient) ImagePull(ctx, ref, docker_types.ImagePullOptions)` |
| `kubernetes.go` | 41 | `func (w *K8sWrapper) Pull(_, _, docker_types.ImagePullOptions)` |
| `docker_test.go` | 240, 249, 290 | Test assertions using `types.ImagePullOptions{}` |

**Also affected types** (used elsewhere in the same files):
- `docker_types.ImageInspect` → still exists in v28 at `image.InspectResponse`
- `docker_types.HijackedResponse` → moved to `docker_types.HijackedResponse` (may still work)
- `docker_types.Ping` → still exists

**Fix:**
```go
// Before (pkg/container/client.go)
import docker_types "github.com/docker/docker/api/types"
ImagePull(ctx context.Context, ref string, options docker_types.ImagePullOptions) (io.ReadCloser, error)

// After
import "github.com/docker/docker/api/types/image"
ImagePull(ctx context.Context, ref string, options image.PullOptions) (io.ReadCloser, error)
```

Then remove the `replace github.com/docker/docker` directive from transferia's go.mod and let it resolve to v28 natively.

## Issue 2: kafka-go patched fork fields

**transferia's go.mod:**
```
github.com/segmentio/kafka-go v0.4.48
replace github.com/segmentio/kafka-go => ./vendor_patched/github.com/segmentio/kafka-go
```

transferia uses a local patched fork of `segmentio/kafka-go` that adds two custom fields to `kafka.Writer`:

| File | Line | Field |
|---|---|---|
| `pkg/providers/kafka/writer/writer_impl.go` | 39 | `AutoDeriveBatchBytes: true` |
| `pkg/providers/kafka/writer/writer_impl.go` | 40 | `ApplyBatchBytesAfterCompression: true` |

These fields don't exist in upstream `segmentio/kafka-go v0.4.48`. Downstream modules resolve the upstream version, causing compile failure.

**Fix options:**
1. **Upstream the patch:** Submit PR to `segmentio/kafka-go` adding `AutoDeriveBatchBytes` and `ApplyBatchBytesAfterCompression`
2. **Publish a fork:** Push the patched version to `github.com/transferia/kafka-go` and use a permanent replace pointing there (this is transitive-safe if downstream also adds the replace)
3. **Conditional compilation:** Use build tags to gate the patched fields
4. **Remove the fields:** If they're not critical, remove them and adjust batch size logic

## Impact

Both issues block compilation of any test package that transitively imports:
- `transferia/pkg/container` (pulled via `tests/helpers` → `pkg/worker` → `pkg/container`)
- `transferia/pkg/providers/kafka/writer` (pulled via kafka provider imports)

The **main library code** (`go build ./...`) compiles fine because it doesn't import these packages directly. Only **test binaries** (`go test ./tests/...`) fail.

## Recommended fix

In the transferia main repo:
1. Fix `pkg/container/*.go` to use `image.PullOptions` from Docker v28 API
2. Remove `replace github.com/docker/docker` from go.mod
3. Either upstream the kafka-go patch or publish `github.com/transferia/kafka-go` fork
4. Replace `replace ./vendor_patched/...` with `replace github.com/segmentio/kafka-go => github.com/transferia/kafka-go v0.4.48-patched`
5. Release as `transferia v0.0.6` (or `v0.0.7`)

After that, downstream modules like `transferia/iceberg` can import transferia without any special replace directives and all test packages will compile.
