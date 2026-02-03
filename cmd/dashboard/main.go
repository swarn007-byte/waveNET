package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"wavenet/internal/model"
)

type hub struct {
	mu      sync.Mutex
	events  []model.DashboardEvent
	clients map[chan model.DashboardEvent]struct{}
}

func newHub() *hub {
	return &hub{
		clients: make(map[chan model.DashboardEvent]struct{}),
	}
}

func (h *hub) add(event model.DashboardEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.events = append(h.events, event)
	if len(h.events) > 200 {
		h.events = h.events[len(h.events)-200:]
	}

	for ch := range h.clients {
		select {
		case ch <- event:
		default:
		}
	}
}

func (h *hub) snapshot() []model.DashboardEvent {
	h.mu.Lock()
	defer h.mu.Unlock()

	out := make([]model.DashboardEvent, len(h.events))
	copy(out, h.events)
	return out
}

func (h *hub) subscribe() chan model.DashboardEvent {
	h.mu.Lock()
	defer h.mu.Unlock()

	ch := make(chan model.DashboardEvent, 16)
	h.clients[ch] = struct{}{}
	return ch
}

func (h *hub) unsubscribe(ch chan model.DashboardEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, ch)
	close(ch)
}

func main() {
	h := newHub()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, dashboardHTML)
	})

	http.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(h.snapshot())
			return
		}

		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		defer r.Body.Close()
		var event model.DashboardEvent
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		h.add(event)
		w.WriteHeader(http.StatusAccepted)
	})

	http.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		ch := h.subscribe()
		defer h.unsubscribe(ch)

		for _, event := range h.snapshot() {
			writeSSE(w, event)
		}
		flusher.Flush()

		for {
			select {
			case <-r.Context().Done():
				return
			case event := <-ch:
				writeSSE(w, event)
				flusher.Flush()
			}
		}
	})

	log.Println("Dashboard listening on http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func writeSSE(w http.ResponseWriter, event model.DashboardEvent) {
	body, _ := json.Marshal(event)
	fmt.Fprintf(w, "data: %s\n\n", body)
}

const dashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>WaveNET Dashboard</title>
  <style>
    :root {
      --bg: #f3efe6;
      --panel: rgba(255, 252, 244, 0.94);
      --ink: #1f2a2c;
      --accent: #0d6c63;
      --accent-soft: #d7efe7;
      --warn: #a13f1d;
      --grid: rgba(13, 108, 99, 0.15);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Avenir Next", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(13, 108, 99, 0.18), transparent 30%),
        linear-gradient(135deg, #f8f4eb, var(--bg));
      min-height: 100vh;
    }
    .wrap {
      max-width: 1100px;
      margin: 0 auto;
      padding: 24px;
    }
    .hero {
      margin-bottom: 20px;
      padding: 24px;
      border-radius: 24px;
      background: var(--panel);
      box-shadow: 0 20px 60px rgba(41, 56, 58, 0.12);
      border: 1px solid rgba(31, 42, 44, 0.08);
    }
    h1 {
      margin: 0 0 8px;
      font-size: clamp(2rem, 4vw, 3.4rem);
      letter-spacing: -0.05em;
    }
    .sub {
      margin: 0;
      max-width: 60ch;
      line-height: 1.5;
    }
    .grid {
      display: grid;
      grid-template-columns: 280px 1fr;
      gap: 20px;
    }
    .card {
      background: var(--panel);
      border-radius: 20px;
      padding: 18px;
      border: 1px solid rgba(31, 42, 44, 0.08);
      box-shadow: 0 14px 30px rgba(41, 56, 58, 0.08);
    }
    .nodes {
      display: grid;
      gap: 10px;
    }
    .node {
      padding: 12px 14px;
      border-radius: 14px;
      background:
        linear-gradient(135deg, rgba(13, 108, 99, 0.14), rgba(13, 108, 99, 0.05));
      border: 1px solid rgba(13, 108, 99, 0.18);
      animation: pulseIn 0.45s ease;
    }
    .feed {
      display: grid;
      gap: 12px;
      max-height: 65vh;
      overflow: auto;
      padding-right: 6px;
    }
    .event {
      padding: 14px;
      border-radius: 16px;
      background-image:
        linear-gradient(135deg, rgba(255,255,255,0.8), rgba(215,239,231,0.75)),
        linear-gradient(90deg, var(--grid) 1px, transparent 1px),
        linear-gradient(var(--grid) 1px, transparent 1px);
      background-size: auto, 24px 24px, 24px 24px;
      border: 1px solid rgba(13, 108, 99, 0.14);
      animation: slideUp 0.4s ease;
    }
    .event strong {
      color: var(--accent);
    }
    .warn strong {
      color: var(--warn);
    }
    .meta {
      margin-top: 8px;
      font-size: 0.92rem;
      opacity: 0.82;
    }
    @keyframes slideUp {
      from { opacity: 0; transform: translateY(10px); }
      to { opacity: 1; transform: translateY(0); }
    }
    @keyframes pulseIn {
      from { opacity: 0; transform: scale(0.96); }
      to { opacity: 1; transform: scale(1); }
    }
    @media (max-width: 860px) {
      .grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>WaveNET Live Flow</h1>
      <p class="sub">Every card below is a real event sent by a node on the LAN. This makes the message flood visible without watching several terminals at once.</p>
    </section>

    <section class="grid">
      <div class="card">
        <h2>Seen Nodes</h2>
        <div id="nodes" class="nodes"></div>
      </div>
      <div class="card">
        <h2>Propagation Feed</h2>
        <div id="feed" class="feed"></div>
      </div>
    </section>
  </div>

  <script>
    const nodes = new Map();
    const nodesEl = document.getElementById("nodes");
    const feedEl = document.getElementById("feed");

    function renderNodes() {
      nodesEl.innerHTML = "";
      [...nodes.keys()].sort().forEach((name) => {
        const div = document.createElement("div");
        div.className = "node";
        div.textContent = name;
        nodesEl.appendChild(div);
      });
    }

    function addEvent(event) {
      nodes.set(event.node, true);
      renderNodes();

      const card = document.createElement("div");
      card.className = "event";
      if (event.kind.includes("failed") || event.kind === "dropped") {
        card.classList.add("warn");
      }
      const path = event.path && event.path.length ? event.path.join(" -> ") : "No path yet";
      const peers = event.peers && event.peers.length ? event.peers.join(", ") : "None";
      card.innerHTML = "<strong>[" + event.node + "] " + event.kind + "</strong><div>" +
        (event.message_id || "no-message") + " | " + event.detail + "</div>" +
        "<div class='meta'>TTL: " + event.ttl + " | Path: " + path + " | Peers: " + peers + "</div>";
      feedEl.prepend(card);
    }

    fetch("/events")
      .then((res) => res.json())
      .then((events) => events.forEach(addEvent));

    const stream = new EventSource("/stream");
    stream.onmessage = (message) => addEvent(JSON.parse(message.data));
  </script>
</body>
</html>`
