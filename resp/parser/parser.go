package parser

import (
	"bufio"
	"errors"
	"go-redis/interface/resp"
	"go-redis/resp/reply"
	"io"
	"strconv"
	"strings"
)

/*
	解析客户端与服务端之间通信时的Arg参数
 */

// Payload 承载 解析出来的结果。
type Payload struct {
	Data resp.Reply
	Err error
}

type readState struct {
	readingMultiLine    bool    // 是否是在读取多行消息
	expectedArgsCount   int     // 如果是array, 需要读多少个参数
	msgType             byte    // + - * $ :
	args                [][]byte    // 已经读到的参数
	bulkLen             int     // 如果是bulk string 还要记录它的长度
}

func (rs *readState) finished() bool {
	return rs.expectedArgsCount > 0 && rs.expectedArgsCount == len(rs.args)
}


func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}


func parse0(reader io.Reader, ch chan<- *Payload) {
	bufReader := bufio.NewReader(reader)
	state := &readState{}
	for {
		msg, ioErr, err := readLine(bufReader, state)
		if err != nil {
			if ioErr {
				ch <- &Payload{
					Err: err,
				}
				close(ch)
				return
			}
			ch <- &Payload{
				Err: err,
			}
			*state = readState{} // 清空状态, 准备下一次读取
			continue
		}
		// readLine 没出错
		if !state.readingMultiLine { // 刚开始还没有设置多行解析（false），下面的parseMultiBulkHeader会把readingMultiLine置位true
			if msg[0] == '*' {
				err = parseMultiBulkHeader(msg, state)
				if err != nil { // 协议错误直接continue，等待下一次接收消息，不用断开连接
					ch <- &Payload{
						Err:err,
					}
					*state = readState{}
					continue
				}
				// parseMultiBulkHeader没有出错，说明state（解析器）中的状态已经被成功修改，接下来就是读取每一行
				if state.expectedArgsCount == 0 {
					ch <-&Payload{
						Data: &reply.NullMultiBulkReply{},
					}
					*state = readState{}
					continue
				}
			} else if msg[0] == '$' {
				err = parseBulkHeader(msg, state)
				if err != nil{
					ch <- &Payload{
						Err:err,
					}
					*state = readState{}
					continue
				}
				if state.bulkLen == -1 {
					ch <- &Payload{
						Data: reply.NewNullBulkReply(),
					}
					*state = readState{}
					continue
				}
			} else {  // 除了数组和批量字符串是多行之外，其他的都是单行读取
				var r resp.Reply
				r, err = parseSingleLineReply(msg, state)
				ch <- &Payload{
					Data: r,
					Err: err,
				}
				*state = readState{}
				continue
			}
		} else {
			err = readBody(msg, state)
			if err != nil {
				ch <- &Payload{
					Err: err,
				}
				*state = readState{}
				continue
			}
			if state.finished() {
				var r resp.Reply
				if state.msgType == '*' {
					r = reply.NewMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					r = reply.NewBulkReply(state.args[0])
				}
				ch <- &Payload{
					Data: r,
					Err: nil,
				}
				*state = readState{}
				continue
			}

		}
	}
}

func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) {
	var msg []byte
	var err error

	if state.bulkLen == 0 {
		msg, err = bufReader.ReadBytes('\n')
		if err != nil { // io错误
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg) - 2] != '\r' { // 协议错误
			return nil, false, errors.New("protocol error" + string(msg))
		}
	} else {
		msg = make([]byte, state.bulkLen+2)
		_, err = io.ReadFull(bufReader, msg)
		if err != nil { // io错误
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg) - 2] != '\r' || msg[len(msg) - 1] != '\n' { // 协议错误
			return nil, false, errors.New("protocol error" + string(msg))
		}
		state.bulkLen = 0
	}
	return msg, false , nil
}

// parseMultiBulkHeader
// 查看第二个字符是不是数字，不是数字直接报错
// 是数字：如果等于0，说明期望得到的命令数==0，返回
//      如果大于0，将消息类型赋值给readState
func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedLine uint64
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg) - 2]), 10, 64)
	if err != nil {
		return errors.New("protocol error" + string(msg))
	}
	if expectedLine == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = int(expectedLine)
		state.args = make([][]byte, 0, expectedLine)
		return nil
	} else {
		return errors.New("protocol error" + string(msg))
	}
}

func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedLine int64
	expectedLine, err = strconv.ParseInt(string(msg[1:len(msg) - 2]), 10, 64)
	state.bulkLen = int(expectedLine)
	if err != nil {
		return errors.New("protocol error" + string(msg))
	}
	if state.bulkLen == -1 {
		return nil
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
		return nil
	} else {
		return errors.New("protocol error" + string(msg))
	}
}

// parseSingleLineReply +OK\r\n -Err<message>\r\n
func parseSingleLineReply(msg []byte, state *readState) (resp.Reply, error) {
	var err error
	var r resp.Reply
	str := strings.TrimSuffix(string(msg), "\r\n")
	switch msg[0] {
	case '+':
		r = reply.NewStatusReply(str[1:])
	case '-':
		r = reply.NewErrReply(str[1:])
	case ':':
		var code int64
		code, err = strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, err
		}
		r = reply.NewIntReply(code)
	default:
		err = errors.New("protocol error" + string(msg))
	}
	return r, err
}
// readBody
// $3\r\n
// SET\r\n
// $3\r\n
// key\r\n
// $5\r\n
// value\r\n
// $0\r\n
// \r\n
func readBody(msg []byte, state *readState) (err error) {
	line := msg[:len(msg) - 2]
	if line[0] == '$' {
		var code int64
		code, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error" + string(msg))
		}
		if code <= 0 {
			state.bulkLen = 0
			state.args = append(state.args, []byte{})
		} else {
			state.bulkLen = int(code)
		}
	} else {
		state.args = append(state.args, line)
	}
	return nil
}


