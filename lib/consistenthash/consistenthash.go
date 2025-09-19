package consistenthash

import (
	"hash/crc32"
	"sort"
)

type HashFunc func([]byte) uint32

type NodeMap struct {
	hashFunc HashFunc
	nodeHashes []int
	nodeHashMap map[int]string
}

func NewNodeMap(hf HashFunc) *NodeMap {
	n := &NodeMap{
		hashFunc: hf,
		nodeHashMap: make(map[int]string),
	}
	if n.hashFunc == nil {
		n.hashFunc = crc32.ChecksumIEEE
	}
	return n
}

func (m *NodeMap) IsEmpty() bool {
	return len(m.nodeHashMap) == 0
}


func (m *NodeMap) AddNodes(keys ...string) {
	for _, key := range keys {
		if key == "" {
			continue
		}
		hashNum := int(m.hashFunc([]byte(key)))
		m.nodeHashes = append(m.nodeHashes, hashNum)
		m.nodeHashMap[hashNum] = key
	}
	sort.Ints(m.nodeHashes)
}

func (m *NodeMap) PickNode(key string) string {
	if m.IsEmpty() {
		 return ""
	}

	hash := int(m.hashFunc([]byte(key)))
	target := m.nodeHashes[0]
	for _, node := range m.nodeHashes {
		if node >= hash {
			target = node
			break
		}
	}
	return m.nodeHashMap[target]
}


