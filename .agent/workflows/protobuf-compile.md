---
description: Compile protobuf files after any changes to proto/ directory
---

# Protobuf Compilation Workflow

**IMPORTANT:** Whenever you make changes to any `.proto` file in the `proto/` directory, you MUST run the protoc compiler to regenerate the Go code.

**Fixed output rule (do not deviate):**
- `proto/*.proto` -> `xmeta/*.pb.go`
- Always run protoc from `proto/` with the explicit `--go_out=../xmeta` target.

## Compilation Steps

// turbo-all

0. Run from `proto/`:
```bash
cd /home/peter/Workspace/sqlmeta/proto
```

1. Re-generate all sqlmeta protos:
```bash
protoc -I=. --go_out=../xmeta --go_opt=paths=source_relative *.proto
```

## Verification

After compiling, verify the output files exist in `xmeta/` (for example):
- `xmeta/types.pb.go`
- `xmeta/pg_meta.pb.go`
- `xmeta/my_meta.pb.go`
- `xmeta/bq_meta.pb.go`
- `xmeta/sqlite_meta.pb.go`

## Common Issues

1. **Wrong output location**: The output location is fixed to `xmeta/`. Do NOT use `--go_out=.`.
2. **Missing protoc**: Ensure `protoc` and `protoc-gen-go` are installed:
   ```bash
   which protoc
   which protoc-gen-go
   ```
