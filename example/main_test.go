package main_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/IsaacDSC/queue/internal/redilson"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnqueueAndListen(t *testing.T) {
	walFile := filepath.Join(t.TempDir(), "redilson.wal")
	queue := redilson.NewQueueWithWAL(walFile)

	const channelName = "channelTest"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, queue.CreateChannel(ctx, channelName))

	expected := []any{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	received := make([]any, 0, len(expected))
	var mu sync.Mutex
	done := make(chan struct{})

	go func() {
		_ = queue.Listener(ctx, channelName, func(ctx context.Context, value any) error {
			mu.Lock()
			received = append(received, value)
			n := len(received)
			mu.Unlock()

			if n == len(expected) {
				close(done)
			}
			return nil
		})
	}()

	for i := range 10 {
		require.NoError(t, queue.Enqueue(ctx, channelName, i))
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for messages")
	}

	cancel()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, expected, received)
}
