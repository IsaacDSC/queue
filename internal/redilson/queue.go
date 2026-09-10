package redilson

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/IsaacDSC/queue/pkg/date"
)

type Queue struct {
	mu    sync.Mutex
	wal   *WAL
	queue map[string]*list.List
}

func NewQueue() *Queue {
	return NewQueueWithWAL(walPath)
}

func NewQueueWithWAL(path string) *Queue {
	q := &Queue{wal: NewWal(path), queue: make(map[string]*list.List)}

	// Restore in-memory only — writing to WAL here would duplicate records.
	if err := q.wal.Load(func(op Operation, channel string, value any) error {
		switch op {
		case OpCreateChannel:
			q.queue[channel] = list.New()
		case OpEnqueue:
			q.queue[channel].PushBack(value)
		}
		return nil
	}); err != nil {
		panic(err)
	}

	return q
}

var ErrAlreadyExistentChannel = errors.New("error when create because already exist")

func (q *Queue) CreateChannel(ctx context.Context, name string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.queue[name]; ok {
		return ErrAlreadyExistentChannel
	}

	err := q.wal.Write(ctx, Record{
		Op:      OpCreateChannel,
		Channel: name,
		Ts:      date.Now(),
	})
	if err != nil {
		return fmt.Errorf("error on write WAL, whith msg : %w", err)
	}

	q.queue[name] = list.New()
	return nil
}

var ErrNotExistChannel = errors.New("error not exist this channel")

func (q *Queue) Enqueue(ctx context.Context, channel string, value any) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	queue, ok := q.queue[channel]
	if !ok {
		return ErrNotExistChannel
	}

	err := q.wal.Write(ctx, Record{
		Op:      OpEnqueue,
		Ts:      date.Now(),
		Channel: channel,
		Value:   value,
	})
	if err != nil {
		return fmt.Errorf("error on write WAL, whith msg : %w", err)
	}

	queue.PushBack(value)
	return nil
}

func (q *Queue) Listener(ctx context.Context, channel string, fn func(ctx context.Context, value any) error) error {
	q.mu.Lock()
	queue, ok := q.queue[channel]
	q.mu.Unlock()
	if !ok {
		return ErrNotExistChannel
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		q.mu.Lock()
		if queue.Len() == 0 {
			q.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Millisecond):
			}
			continue
		}

		front := queue.Front()
		value := front.Value
		q.mu.Unlock()

		if err := fn(ctx, value); err != nil {
			log.Println(err.Error())
			continue
		}

		err := q.wal.Write(ctx, Record{
			Op:      OpAck,
			Ts:      date.Now(),
			Channel: channel,
		})
		if err != nil {
			return fmt.Errorf("error on write WAL, whith msg : %w", err)
		}

		q.mu.Lock()
		// Re-check front in case the list changed; remove only if still present.
		if queue.Front() == front {
			queue.Remove(front)
		}
		q.mu.Unlock()
	}
}
