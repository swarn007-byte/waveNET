package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"wavenet/internal/node"
)

func main() {
	opts, positionals, err := parseArgs(os.Args[1:])
	if err != nil {
		log.Println(err)
		log.Println("Usage: go run ./cmd/node <name> <tcpPort> [--gateway] [--seed=host:port] [--dashboard-url=http://localhost:8080]")
		os.Exit(1)
	}

	if len(positionals) < 2 {
		log.Println("Usage: go run ./cmd/node <name> <tcpPort> [--gateway] [--seed=host:port] [--dashboard-url=http://localhost:8080]")
		os.Exit(1)
	}

	name := positionals[0]
	tcpPort := positionals[1]

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := node.Config{
		Name:          name,
		TCPPort:       tcpPort,
		DiscoveryPort: opts.discoveryPort,
		AnnounceEvery: 2 * time.Second,
		PeerTTL:       15 * time.Second,
		GatewayMode:   opts.gatewayMode,
		GatewayLog:    "delivered_messages.log",
		SeedPeers:     splitCSV(opts.seed),
		DashboardURL:  strings.TrimRight(opts.dashboardURL, "/"),
	}

	if err := node.New(cfg).Run(ctx); err != nil {
		log.Fatal(err)
	}
}

type options struct {
	gatewayMode   bool
	seed          string
	discoveryPort string
	dashboardURL  string
}

func parseArgs(args []string) (options, []string, error) {
	opts := options{
		discoveryPort: "9999",
	}

	var positionals []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if !strings.HasPrefix(arg, "-") {
			positionals = append(positionals, arg)
			continue
		}

		switch {
		case arg == "-gateway" || arg == "--gateway":
			opts.gatewayMode = true
		case strings.HasPrefix(arg, "--seed=") || strings.HasPrefix(arg, "-seed="):
			opts.seed = strings.SplitN(arg, "=", 2)[1]
		case arg == "--seed" || arg == "-seed":
			i++
			if i >= len(args) {
				return options{}, nil, fmt.Errorf("missing value for %s", arg)
			}
			opts.seed = args[i]
		case strings.HasPrefix(arg, "--discovery-port="):
			opts.discoveryPort = strings.SplitN(arg, "=", 2)[1]
		case arg == "--discovery-port":
			i++
			if i >= len(args) {
				return options{}, nil, fmt.Errorf("missing value for %s", arg)
			}
			opts.discoveryPort = args[i]
		case strings.HasPrefix(arg, "--dashboard-url="):
			opts.dashboardURL = strings.SplitN(arg, "=", 2)[1]
		case arg == "--dashboard-url":
			i++
			if i >= len(args) {
				return options{}, nil, fmt.Errorf("missing value for %s", arg)
			}
			opts.dashboardURL = args[i]
		default:
			return options{}, nil, fmt.Errorf("unknown option: %s", arg)
		}
	}

	return opts, positionals, nil
}

func splitCSV(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}

	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
