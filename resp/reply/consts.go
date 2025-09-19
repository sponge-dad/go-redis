package reply

// PongReply 1. Pong回复
type PongReply struct{}

var pongBytes = []byte("+PONG\r\n")

func NewPongReply() *PongReply {
	return &PongReply{}
}

func (r *PongReply) ToBytes() []byte{
	return pongBytes
}

// Ok
type OkReply struct {
}

var okBytes = []byte("+OK\r\n")
var okReply = new(OkReply)
func NewOkReply() *OkReply {
	return okReply
}

func (r *OkReply) ToBytes() []byte {
	return okBytes
}


// NullBulkReply nil回复  ！！！nil和空字符串不一样
type NullBulkReply struct{}

var nullBulkReply = new(NullBulkReply)
func NewNullBulkReply() *NullBulkReply {
	return nullBulkReply
}

// nil 是 $-1\r\n
// 空字符串 是 $0\r\n\r\n
var nullBulkBytes = []byte("$-1\r\n")

func (r *NullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}

// NullMultiBulkReply 空数组回复
type NullMultiBulkReply struct{}
var nullMultiBulkReply = new(NullMultiBulkReply)
func NewNullMultiBulkReply() *NullMultiBulkReply {
	return nullMultiBulkReply
}

var nullMultiBulkBytes = []byte("*0\r\n")

func (r *NullMultiBulkReply) ToBytes() []byte {
	return nullMultiBulkBytes
}


// NoReply 空回复
type NoReply struct {
}

var noBytes = []byte("")

func (r *NoReply) ToBytes() []byte {
	return noBytes
}












