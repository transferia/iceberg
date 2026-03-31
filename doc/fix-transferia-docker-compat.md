# Fix: transferia Docker client API compatibility

## Problem

`transferia/pkg/container/*.go` uses `types.ImagePullOptions` from the Docker client SDK (`github.com/docker/docker/api/types`). This type was removed/relocated in Docker client v26+:

```
v25.x:  github.com/docker/docker/api/types.ImagePullOptions     ← transferia uses this
v26.x:  github.com/docker/docker/api/types/image.PullOptions     ← moved here
v27+:   github.com/moby/moby (module path changed entirely)
```

Any downstream project that depends on both `transferia` and a modern `testcontainers-go` (v0.34+) or `iceberg-go` (which uses testcontainers v0.41) cannot compile, because:
- `transferia/pkg/container` requires Docker v25 (`types.ImagePullOptions`)
- `testcontainers-go v0.34+` requires Docker v26+ (`container.ExecOptions`, `network.ListOptions`)
- `testcontainers-go v0.41` requires Docker v28+ (`github.com/moby/moby`)

There is no Docker version that satisfies both.

## Affected files in transferia

```
pkg/container/client.go:17       types.ImagePullOptions
pkg/container/container.go:15    types.ImagePullOptions
pkg/container/docker.go:55,58,79 types.ImagePullOptions
pkg/container/docker_mocks.go:23 types.ImagePullOptions
pkg/container/kubernetes.go:41   types.ImagePullOptions
```

## Fix

Replace `types.ImagePullOptions` with `image.PullOptions` from `github.com/docker/docker/api/types/image`:

```go
// Before
import "github.com/docker/docker/api/types"
func pull(opts types.ImagePullOptions) { ... }

// After
import "github.com/docker/docker/api/types/image"
func pull(opts image.PullOptions) { ... }
```

Then bump the Docker client dependency in `transferia`'s go.mod to `v26.x` or later, and run `go mod tidy`.

## Impact

This is a breaking change for any downstream project that:
1. Imports `transferia/pkg/container` directly (unlikely — it's internal infra)
2. Uses `transferia` as a dependency alongside modern Docker/testcontainers libraries (this is the iceberg plugin's situation)

After the fix, release as `transferia v0.0.3` and update `iceberg`'s go.mod to use it.

## Workaround (current)

The `transferia/iceberg` project currently cannot compile test packages that transitively import `pkg/container`. The main library package compiles fine. Only integration tests (which use `helpers.Activate` → `worker` → `testcontainers`) are affected.
