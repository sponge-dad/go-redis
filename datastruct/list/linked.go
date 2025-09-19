package list

import (
	"go-redis/lib/utils"
)

type LinkList struct {
	head *node
	tail *node
	size int
}



type node struct {
	val interface{}
	next *node
	prev *node
}

// 向尾部增加节点
func (l *LinkList) Add(val interface{}) {
	if l == nil {
		panic("list is nil")
	}
	n := &node{
		val: val,
	}
	if l.tail == nil {
		l.head = n
		l.tail = n
	} else {
		n.prev = l.tail
		l.tail.next = n
		l.tail = n
	}
	l.size ++
}

// 查找双向链表中第index(0,1,2,3,...,size-1)个节点，
// 如果大于长度的一半，就从后往前找；否则从前往后找
func (l *LinkList) find (index int) (n *node) {
	if index < l.size / 2 {
		n = l.head
		for i := 0; i < index; i ++ {
			n = n.next
		}
		return n
	} else {
		n = l.tail
		for i := l.size - 1; i > index; i -- {
			n = n.prev
		}
		return n
	}
}


func (l *LinkList) Get (index int) (val interface{}) {
	if l == nil {
		panic("list is nil")
	}
	if index < 0 || index >= l.size {
		panic("index out of bound")
	}
	return l.find(index).val
}


func (l *LinkList) Set(index int, val interface{}) {
	if l == nil {
		panic("list is nil")
	}
	if index < 0 || index >= l.size {
		panic("index out of bound")
	}
	l.find(index).val = val
}

func (l *LinkList) Insert(index int, val interface{}) {
	if l == nil {
		panic("list is nil")
	}
	if index < 0 || index > l.size {
		panic("index out of bound")
	}

	if index == l.size {
		l.Add(val)
		return
	} else {
		pivot := l.find(index)
		n := &node {
			val: val,
			prev: pivot.prev,
			next: pivot,
		}
		if pivot.prev == nil {
			l.head = n
		} else {
			pivot.prev.next = n
		}
		pivot.prev = n
		l.size ++
	}
}


func (l *LinkList) removeNode(n *node) {
	if n.prev == nil {
		l.head = n.next
	} else {
		n.prev.next = n.next
	}

	if n.next == nil {
		l.tail = n.prev
	} else {
		n.next.prev = n.prev
	}
	n.prev = nil
	n.next = nil
	l.size --
}

func (l *LinkList) Remove(index int) (val interface{}){
	if l == nil {
		panic("list is nil")
	}
	if index < 0 || index >= l.size {
		panic("index out of bound")
	}
	n := l.find(index)
	l.removeNode(n)
	return n.val
}

func (l *LinkList) RemoveLast() (val interface{}){
	if l == nil {
		panic("list is nil")
	}
	if l.tail == nil {
		return nil
	}
	n := l.tail
	l.removeNode(l.tail)
	return n.val
}

func (l *LinkList) RemoveAllByVal (val interface{}) int {
	if l == nil {
		panic("list is nil")
	}
	removed := 0
	for n := l.head; n != nil; {
		next := n.next
		if utils.Equals(val, n.val) {
			l.removeNode(n)
			removed ++
		}
		n = next
	}
	return removed
}

func (l *LinkList) RemoveByVal(val interface{}, count int) int {
	if l == nil {
		panic("list is nil")
	}
	removed := 0
	for n := l.head; n != nil; {
		next := n.next
		if utils.Equals(val, n.val) {
			l.removeNode(n)
			removed ++
		}
		if removed == count {
			break
		}
		n = next
	}
	return removed
}

func (l *LinkList) ReverseRemoveByVal(val interface{}, count int) int {
	if l == nil {
		panic("list is nil")
	}
	removed := 0
	for n := l.tail; n != nil; {
		prev := n.prev
		if utils.Equals(val, n.val) {
			l.removeNode(n)
			removed ++
		}
		if removed == count {
			break
		}
		n = prev
	}
	return removed
}


func (l *LinkList) Len() int {
	if l == nil {
		panic("list is nil")
	}
	return l.size
}


func (l *LinkList) ForEach(consumer func(int, interface{}) bool) {
	if l == nil {
		panic("list is nil")
	}
	n := l.head
	i := 0
	for n != nil {
		goNext := consumer(i, n.val)
		if !goNext || n.next == nil {
			break
		}
		i ++
		n = n.next
	}
}

func (l *LinkList) Contains(val interface{}) bool {
	if l == nil {
		panic("list is nil")
	}
	contains := false
	l.ForEach(func(index int, v interface{}) bool {
		if utils.Equals(v, val) {
			contains = true
			return false
		}
		return true
	})
	return contains
}

func (l *LinkList) Range(start, stop int) []interface{} {
	if l == nil {
		panic("list is nil")
	}
	if start < 0 || start >= l.size {
		panic("`start` out of range")
	}
	if stop < start || stop > l.size {
		panic("`stop` out of range")
	}

	sliceSize := stop - start
	result := make([]interface{}, 0, sliceSize)

	n := l.find(start)
	i := start
	for n != nil && i < stop {
		result = append(result, n.val)
		n = n.next
		i ++
	}
	return result
}


func New(vals ...interface{}) *LinkList {
	l := &LinkList{}
	for _, v := range vals {
		l.Add(v)
	}
	return l
}




















