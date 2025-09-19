package dict

type Consumer func (key string, value interface{}) bool

type Dict interface {
	Get(key string) (val interface{}, exist bool)
	Len() int
	Put(key string, value interface{}) int
	PutIfAbsent(key string, value interface{}) int
	PutIfExist(key string, value interface{}) int
	Remove(key string) int
	ForEach(consumer Consumer)
	Keys() []string
	RandomKeys(limit int) []string
	RandomDistinctKeys(limit int) []string
	Clear()
}
