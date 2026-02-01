package gateway

import (
	"encoding/json"
	"os"
	"sync"
	"time"

	"wavenet/internal/model"
)

// Logger simulates the point where a real deployment would hand the message
// to SMS, satellite, or cloud infrastructure.
type Logger struct {
	mu   sync.Mutex
	path string
}

func NewLogger(path string) *Logger {
	return &Logger{path: path}
}

func (l *Logger) Record(nodeName string, msg model.Message) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	file, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	entry := struct {
		Time    string        `json:"time"`
		Gateway string        `json:"gateway"`
		Message model.Message `json:"message"`
	}{
		Time:    time.Now().Format(time.RFC3339),
		Gateway: nodeName,
		Message: msg,
	}

	return json.NewEncoder(file).Encode(entry)
}
