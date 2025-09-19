package reply

import (
	"fmt"
	"go-redis/interface/resp"
	"strconv"
)

var (
	nullBulkReplyBytes = []byte("$-1\r\n")
	CRLF               = "\r\n"
)

// BulkReply stores a binary-safe string
type BulkReply struct {
	Arg []byte    // "sponge"    "$6\r\nsponge\r\n"
}

func NewBulkReply(arg []byte) *BulkReply {
	return &BulkReply{
		Arg: arg,
	}
}

func (r *BulkReply) ToBytes() []byte {
	if r.Arg == nil {
		return nullBulkBytes
	}
	return []byte(fmt.Sprintf("$%d%s%s%s", len(r.Arg), CRLF, r.Arg, CRLF))
}

/* ---- Multi Bulk Reply ---- */

// MultiBulkReply stores a list of string
type MultiBulkReply struct {
	Args [][]byte
}



func NewMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{
		Args: args,
	}
}

func (r *MultiBulkReply) ToBytes() []byte {
	argLen := len(r.Args)
	reply := make([]byte, 0, 1024)
	reply = append(reply, '*')
	reply = append(reply, strconv.Itoa(argLen)...)
	reply = append(reply, CRLF...)
	for i := 0; i < argLen; i ++ {
		reply = append(reply, '$')
		reply = append(reply, strconv.Itoa(len(r.Args[i]))...)
		reply = append(reply, CRLF...)
		reply = append(reply, r.Args[i]...)
		reply = append(reply, CRLF...)
	}
	return reply
}


/* ---- Status Reply ---- */

// StatusReply stores a simple status string
type StatusReply struct {
	Status string
}

// NewStatusReply creates StatusReply
func NewStatusReply(status string) *StatusReply {
	return &StatusReply{
		Status: status,
	}
}

// ToBytes marshal redis.Reply
func (r *StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + CRLF)
}


/* ---- Int Reply ---- */

// IntReply stores an int64 number
type IntReply struct {
	Code int64
}

// NewIntReply creates int protocol
func NewIntReply(code int64) *IntReply {
	return &IntReply{
		Code: code,
	}
}

// ToBytes marshal redis.Reply
func (r *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}

/* ---- Error Reply ---- */

// ErrorReply is an error and redis.Reply
type ErrorReply interface {
	ToBytes() []byte
	Error() string
}

// StandardErrReply represents server error
type StandardErrReply struct {
	Status string
}

// NewErrReply creates StandardErrReply
func NewErrReply(status string) *StandardErrReply {
	return &StandardErrReply{
		Status: status,
	}
}

// ToBytes marshal redis.Reply
func (r *StandardErrReply) ToBytes() []byte {
	return []byte("-" + r.Status + CRLF)
}

func (r *StandardErrReply) Error() string {
	return r.Status
}


func IsErrReply(reply resp.Reply) bool {
	return reply.ToBytes()[0] == '-'
}






