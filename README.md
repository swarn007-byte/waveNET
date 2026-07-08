# WaveNET

WaveNET is a beginner-friendly Go project that shows how a message can spread across devices on the same local network without needing the internet. Each node discovers nearby peers with UDP, then relays messages with TCP using a flood-style algorithm with TTL and duplicate protection.

Important scope note: this is LAN-based discovery and relay, not a true router-free mesh like BLE or WiFi Direct. Devices still need to be on the same WiFi or hotspot.

## What the project now includes

- Automatic peer discovery over UDP broadcast.
- TCP message relay with TTL-based hop limits.
- Duplicate-message protection with concurrency-safe shared state.
- Optional `-gateway` mode that simulates delivery to an external service and writes to `delivered_messages.log`.
- Optional live dashboard so propagation is visible in a browser.
- Unit tests for the core flood decisions plus a connection-path test using `net.Pipe()`.

## Project layout

```text
WaveNET/
├── cmd/
│   ├── client/
│   │   └── main.go
│   ├── dashboard/
│   │   └── main.go
│   └── node/
│       └── main.go
├── internal/
│   ├── dashboard/
│   │   └── client.go
│   ├── discovery/
│   │   ├── peers.go
│   │   └── service.go
│   ├── flood/
│   │   ├── store.go
│   │   └── store_test.go
│   ├── gateway/
│   │   └── logger.go
│   ├── model/
│   │   └── message.go
│   └── node/
│       ├── node.go
│       └── node_test.go
├── go.mod
└── README.md
```

## How the system works

### 1. Discovery

Every node sends a tiny UDP broadcast announcement every few seconds on a shared discovery port. Other nodes listening on that port learn the sender's IP and TCP port and add it to a live peer list.

### 2. Flood relay

When a node receives a message:

1. It checks whether the message TTL is already exhausted.
2. It checks whether this message ID has already been seen.
3. If the message is valid and new, it marks it as seen.
4. It decreases TTL by 1.
5. It appends its own name to the path.
6. It forwards the message to all currently known peers.

This is simple on purpose. It avoids routing tables and works well for small, changing local networks.

## How to run

### Start the optional dashboard

```bash
go run ./cmd/dashboard
```

Open [http://localhost:8080](http://localhost:8080) in a browser.

### Start nodes on one machine

Open 3 terminals:

```bash
go run ./cmd/node A 9001 --dashboard-url=http://localhost:8080
go run ./cmd/node B 9002 --dashboard-url=http://localhost:8080
go run ./cmd/node C 9003 --gateway --dashboard-url=http://localhost:8080
```

Wait a few seconds so discovery can find the other nodes.

If your operating system is restrictive about localhost UDP broadcast during same-machine testing, use `--seed=127.0.0.1:<port>` as a local fallback. Real multi-device LAN testing is still the main target for discovery.

### Inject a message

```bash
go run ./cmd/client --target=localhost:9001 --id=sos-001 --ttl=5 --payload="Need help near bridge"
```

You should see:

- terminal logs showing discovery, receive, drop, and forward decisions
- browser dashboard events arriving live
- `delivered_messages.log` created when the gateway node receives the message

## Real multi-device test

To run this on actual laptops:

1. Put all devices on the same WiFi network or hotspot.
2. Start one node per device with a unique TCP port.
3. Make sure local firewalls allow inbound UDP on the discovery port and inbound TCP on the node port.
4. Send a message from one machine to its local node.

Example:

```bash
go run ./cmd/node LaptopA 9001
go run ./cmd/node LaptopB 9001
go run ./cmd/node LaptopC 9001 --gateway
```

If UDP broadcast is blocked by the network environment, you can bootstrap from one known peer:

```bash
go run ./cmd/node LaptopB 9001 --seed=192.168.1.5:9001
```

This does not replace discovery. It simply helps the first connection happen in restrictive networks.

## Beginner notes on key code ideas

- `internal/flood/store.go`: uses a mutex so two goroutines cannot update the seen-message map at the same time.
- `internal/discovery/service.go`: runs background loops for UDP announcements, UDP listening, and peer cleanup.
- `internal/node/node.go`: ties everything together and contains the receive/forward flow.
- `internal/gateway/logger.go`: simulates where a real emergency system might hand off the message to SMS, cloud APIs, or another network.

## Tests

Run:

```bash
go test ./...
```

## Design tradeoffs

- Flooding instead of routing tables:
  Easier to explain, resilient to changing topology, and a good fit for a college demo.
- UDP for discovery:
  Lightweight and good for announcements because we do not need a persistent connection just to say "I exist."
- TCP for relay:
  Simple and reliable for sending the actual JSON message between known peers.
- Simulated gateway delivery:
  Good enough to demonstrate where outside-world delivery would happen, without adding unrelated cloud complexity.

## Current limitations

- Works on the same LAN, not a true infrastructure-free mesh.
- No authentication or encryption yet.
- Flooding is intentionally simple, so it is not optimized for large networks.
- Real multi-device success depends on network settings such as firewall rules and hotspot/router client isolation.
