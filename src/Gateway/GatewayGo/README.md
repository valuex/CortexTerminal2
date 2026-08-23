# GatewayGo

Go rewrite of the CortexTerminal2 Gateway (C# / ASP.NET Core 10). Speaks the same
SignalR Hub Protocol (JSON + MessagePack) and SQLite schema as the original so
the existing Worker daemon and Console frontend can connect without
modification.

## Status

This is a port in progress. See `../../docs/plans/purrfect-dazzling-tower.md`
for the full plan, porting order, and verification checklist.

Current slice: scaffold + 62 HTTP route stubs + static fallback. All routes
return `501 Not Implemented` until the corresponding subsystem is wired up.

## Build & run

```bash
go build ./cmd/gateway
./gateway -config config.example.yaml
```

Listens on `:5045` by default. SQLite database file is created on first run;
all 9 tables are provisioned by embedded SQL migrations.

## Layout

```
cmd/gateway/main.go          entry point, wires config + logger + DB + router
internal/
  server/                    chi router, middleware, static
  config/                    YAML + env post-bind
  data/                      SQLite repos + migrations
  signalr/                   Hand-rolled Hub Protocol (JSON + MessagePack)
  ws/                        Native WebSocket transport for /ws/terminal
  auth/                      JWT, OAuth (GitHub/Google/Apple/Huawei), phone, captcha
  workers/                   Worker registry + SignalR dispatchers
  sessions/                  Session coordinator, replay, artifact, agent
  tunnels/                   Tunnel middleware + registry + quota
  storage/                   S3-compatible artifact storage
  tts/                       Cloudflare + Aliyun TTS
  stats/                     Gateway + session stats
  audit/                     Audit log + request extensions + controller
  support/                   Support info + feedback uploads
  version/                   Release version cache
```

## Wire protocol

JWT (HS256, iss=`https://gateway.local/`, aud=`corterm-gateway` or
`cortex-terminal-gateway`), SignalR Hub Protocol over WebSockets, MessagePack
codec with custom DateTimeOffset ext-type `0x13` (100ns ticks since 0001-01-01
+ 5-byte offset) to match the C# `MessagePack` library byte-for-byte.
