package redilson

import (
	"bufio"
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const walPath = "tmp/redilson.wal"

type WAL struct {
	mu   sync.Mutex
	file *os.File
	path string
}

func NewWal(path string) *WAL {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		panic(err)
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		panic(err)
	}

	return &WAL{file: file, path: path}
}

func (w *WAL) Load(fn func(Op Operation, channel string, value any) error) error {
	channels, pending, err := w.replay()
	if err != nil {
		return err
	}

	for _, channel := range channels {
		if err := fn(OpCreateChannel, channel, nil); err != nil {
			return err
		}

		for el := pending[channel].Front(); el != nil; el = el.Next() {
			if err := fn(OpEnqueue, channel, el.Value); err != nil {
				return err
			}
		}
	}

	return nil
}

func (w *WAL) replay() ([]string, map[string]*list.List, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Sync(); err != nil {
		return nil, nil, err
	}

	file, err := os.Open(w.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	defer file.Close()

	channels := make([]string, 0)
	pending := make(map[string]*list.List)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var record Record
		if err := json.Unmarshal(line, &record); err != nil {
			return nil, nil, fmt.Errorf("error decoding WAL record: %w", err)
		}

		switch record.Op {
		case OpCreateChannel:
			if _, ok := pending[record.Channel]; !ok {
				channels = append(channels, record.Channel)
				pending[record.Channel] = list.New()
			}
		case OpEnqueue:
			q, ok := pending[record.Channel]
			if !ok {
				channels = append(channels, record.Channel)
				q = list.New()
				pending[record.Channel] = q
			}
			q.PushBack(record.Value)
		case OpAck:
			q, ok := pending[record.Channel]
			if !ok || q.Len() == 0 {
				continue
			}
			q.Remove(q.Front())
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}

	return channels, pending, nil
}

func (w *WAL) Write(ctx context.Context, record Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := json.Marshal(record)
	if err != nil {
		return err
	}

	data = append(data, '\n')

	if _, err := w.file.Write(data); err != nil {
		return err
	}

	return w.file.Sync()
}
