package main

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/IsaacDSC/queue/internal/redilson"
)

const channelName = "channelTest"

func main() {
	ctx := context.Background()
	walFile := filepath.Join("tmp", "redilson.wal")
	queue := redilson.NewQueue(walFile)

	if err := queue.CreateChannel(ctx, channelName); err != nil && err != redilson.ErrAlreadyExistentChannel {
		panic(err)
	}

	handler := func(ctx context.Context, value any) error {
		fmt.Println(value)
		return nil
	}

	go func() {
		queue.Listener(ctx, channelName, handler)
	}()

	for i := range 10 {
		if err := queue.Enqueue(ctx, channelName, i); err != nil {
			panic(err)
		}
	}

	time.Sleep(time.Second * 2)
}
