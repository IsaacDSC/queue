package redilson

import "time"

type Operation string

const (
	OpCreateChannel Operation = "create_channel"
	OpEnqueue       Operation = "enqueue"
	OpAck           Operation = "ack"
)

type Record struct {
	Op      Operation
	Channel string
	Value   any
	Ts      time.Time
}
