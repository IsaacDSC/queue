package main_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/IsaacDSC/queue/internal/redilson"
	"github.com/stretchr/testify/assert"
)

const channelName = "channelTest"

func TestX(t *testing.T) {
	ctx := context.Background()
	queue := redilson.NewQueue()

	if err := queue.CreateChannel(ctx, channelName); err != nil && err != redilson.ErrAlreadyExistentChannel {
		panic(err)
	}

	expecteds := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	handler := func(ctx context.Context, value any) error {
		assert.Contains(t, expecteds, value)
		fmt.Println(value)
		return nil
	}

	go func() {
		queue.Listener(ctx, channelName, handler)
	}()

	for i := range 10 {
		queue.Enqueue(ctx, channelName, i)
	}

	time.Sleep(time.Second * 20)

}
