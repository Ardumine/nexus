# HCore.Packages.Nexus — AFCP Connector

The **AFCP Nexus connector**: the HCore driver module that bridges the kernel VFS
to remote peers over [AFCP](https://github.com/Ardumine/afcp). It serves the local
tree (`/proc`, `/etc`, `/dev`, `/packs`, ...) over TCP and mounts remote peers'
trees as read-write VFS mounts (9P-style: remoteness is a path prefix).

Standalone package repo — clones **alongside** the kernel (`ardumine/hcore`) and the
serializer (`ardumine/kaserializer`):

```
ardumine/
  hcore/         ← kernel
  kaserializer/  ← reflection-free serializer (referenced)
  nexus/         ← this repo
```

## What it provides

- Module `HCore.Packages.Nexus.Nexus` implementing `IAfcpKernel` (the shell-facing
  `afcp serve|mount|unmount|status|test` surface), `IDriverModule` (privileged
  kernel doors), and `IRemoteMountHook` (transparent remote subscribe / MKCall).
- Registered by the kernel as the `@afcp` kernel service and the remote-mount hook.

### AFCP layers (all implemented)

| Layer | Capability |
|---|---|
| 1 | Remote VFS mount + `Sync`/`Read`/`Write`/`MkDir`/`Remove` |
| 2 | Transparent `Data.Subscribe<T>` push over the wire |
| 3 | `GetModuleInterface<T>(remotePath)` MKCall marshalling proxy |

Typed wire errors (`AfcpErrorCode`, §C7d) and chunked large-file streaming
(§C7e) are included. The AFCP protocol/transport is bundled in-tree under `AFCP/`
(the upstream-lib swap, §E2.1, is a future task).

## Build

```bash
dotnet build          # builds + deploys to ../hcore/FS/packs/HCore.Packages.Nexus/
```

The kernel spawns the connector at boot (`Program.Main`), so no service file is
needed. Verify end-to-end with the loopback self-test from the kernel shell:

```
afcp test
```

## Layout

- `AFCP/` — bundled AFCP protocol + transport + multiplex + framing.
- `Nexus/` — `AfcpImplement` (the module), `VfsAfcpProvider` (serve side), `ModDescriptor`.
- `Vfs/` — `RemoteFileSystem` (mount side) + `RemoteModuleProxy` (MKCall proxy).
- `Internal/` — `FastMethodInfo` (compiled-delegate method invoker).
