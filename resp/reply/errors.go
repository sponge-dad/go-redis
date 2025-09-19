package reply

import (
	"fmt"
)

// UnknownErrReply 未知错误
type UnknownErrReply struct {
}

func NewUnknownErrReply() *UnknownErrReply {
	return &UnknownErrReply{}
}

var unknownBytes = []byte("-Err unknown\r\n")

func (r *UnknownErrReply) ToBytes() []byte {
	return unknownBytes
}

func (r *UnknownErrReply) Error() string {
	return "Err unknown"
}



type ArgNumErrReply struct {
	cmd string
}

func NewArgNumErrReply(cmd string) *ArgNumErrReply {
	return &ArgNumErrReply{
		cmd: cmd,
	}
}

func (r *ArgNumErrReply) ToBytes() []byte {
	return []byte("-ERR wrong number of arguments for '" + r.cmd + "' command\r\n")
}

func (r *ArgNumErrReply) Error() string {
	return fmt.Sprintf("wrong number of arguments for '%s' command", r.cmd)
}


// SyntaxErrReply represents meeting unexpected arguments
type SyntaxErrReply struct{}

var syntaxErrBytes = []byte("-Err syntax error\r\n")
var theSyntaxErrReply = &SyntaxErrReply{}

// NewSyntaxErrReply creates syntax error
func NewSyntaxErrReply() *SyntaxErrReply {
	return theSyntaxErrReply
}

// ToBytes marshals redis.Reply
func (r *SyntaxErrReply) ToBytes() []byte {
	return syntaxErrBytes
}

func (r *SyntaxErrReply) Error() string {
	return "Err syntax error"
}


// WrongTypeErrReply represents operation against a key holding the wrong kind of value
type WrongTypeErrReply struct{}

var wrongTypeErrBytes = []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")

// ToBytes marshals redis.Reply
func (r *WrongTypeErrReply) ToBytes() []byte {
	return wrongTypeErrBytes
}

func (r *WrongTypeErrReply) Error() string {
	return "WRONGTYPE Operation against a key holding the wrong kind of value"
}








