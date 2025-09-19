package dict

import (
	"sync"
)

type SyncDict struct {
	m sync.Map
}

func NewSyncDict() *SyncDict {
	return &SyncDict{
		m: sync.Map{},
	}
}


func (d *SyncDict) Get(key string) (val interface{}, exist bool) {
	return d.m.Load(key)
}

func (d *SyncDict) Len() int {
	length := 0
	d.m.Range(func(key, value any) bool {
		length ++
		return true
	})
	return length
}

// Put 向SyncDict中插入一个新的键值对
// 如果key不存在，插入后返回1，存在的话，修改成新值，返回0
func (d *SyncDict) Put(key string, value interface{}) int {
	_, exist := d.Get(key)
	d.m.Store(key, value)
	if exist {
		return 0
	}
	return 1
}

func (d *SyncDict) PutIfAbsent(key string, value interface{}) int {
	_, exist := d.Get(key)
	if !exist {
		d.m.Store(key, value)
		return 1
	}
	return 0
}

func (d *SyncDict) PutIfExist(key string, value interface{}) int {
	_, exist := d.Get(key)
	if exist {
		d.m.Store(key, value)
		return 1
	}
	return 0
}

func (d *SyncDict) Remove(key string) int {
	_, exist := d.Get(key)
	if !exist {
		return 0
	}
	d.m.Delete(key)
	return 1
}

func (d *SyncDict) ForEach(consumer Consumer) {
	d.m.Range(func(key, value any) bool {
		return consumer(key.(string), value)
	})
}

func (d *SyncDict) Keys() []string {
	keys := make([]string, 0, d.Len())
	d.ForEach(func(key string, value interface{}) bool {
		keys = append(keys, key)
		return true
	})
	return keys
}

func (d *SyncDict) RandomKeys(limit int) []string {
	keys := make([]string, limit)
	for i := 0; i < limit; i ++ {
		d.m.Range(func(key, value any) bool { // 因为map每次读取的顺序是随机的
			keys[i] = key.(string)
			return false
		})
	}
	return keys
}

func (d *SyncDict) RandomDistinctKeys(limit int) []string {
	if limit > d.Len() || limit <= 0{
		return nil
	}
	keys := make([]string, limit)
	i := 0
	d.m.Range(func(key, value any) bool {
		keys[i] = key.(string)
		i ++
		if i == limit {
			return false
		}
		return true
	})
	return keys
}

func (d *SyncDict) Clear() {
	d.m.Clear()
}



