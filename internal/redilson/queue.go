package redilson

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/IsaacDSC/queue/pkg/date"
)

type Queue struct {
	wal   *WAL
	queue map[string]*list.List
}

func NewQueue() *Queue {
	q := &Queue{wal: NewWal(), queue: make(map[string]*list.List)}

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
	_, ok := q.queue[name]

	if ok {
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
	queue, ok := q.queue[channel]

	if !ok {
		return ErrNotExistChannel
	}

	for {
		if queue.Len() == 0 {
			continue
		}

		front := queue.Front()
		if err := fn(ctx, front.Value); err != nil {
			log.Println(err.Error())
			continue
		}

		err := q.wal.Write(ctx, Record{
			Op:      OpAck,
			Ts:      date.Now(),
			Channel: channel,
		})

		if err != nil {
			panic(fmt.Errorf("error on write WAL, whith msg : %w", err))
		}

		queue.Remove(front)
	}

}
