package index

import (
	"bytes"
	"github.com/google/btree"
	"github.com/orangle/bitcask-k1/disk"
)

// Indexer 索引接口, 内存中索引的定义
type Indexer interface {
	// Put 写入内存索引，在磁盘上保存之后得到的位置记录在内存中
	Put(key []byte, pos *disk.LogRecordPos) *disk.LogRecordPos

	// Get 获取key在磁盘中的位置，这个位置用来读取真正的value
	Get(key []byte) *disk.LogRecordPos

	// Delete 删除内存索引, 在磁盘上删除之后，内存中的索引也要删除
	Delete(key []byte) (*disk.LogRecordPos, bool)

	// Size 索引中的数据量
	Size() int

	// Iterator 索引迭代器
	Iterator(reverse bool) Iterator

	// Close 关闭索引
	Close() error
}

type Item struct {
	key []byte
	pos *disk.LogRecordPos
}

func (item *Item) Less(bi btree.Item) bool {
	return bytes.Compare(item.key, bi.(*Item).key) == -1
}

// Iterator 索引的迭代器
type Iterator interface {
	// Rewind 重新回到迭代器的起点
	Rewind()

	// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
	Seek(key []byte)

	// Next 跳转到下一个 key
	Next()

	// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
	Valid() bool

	// Key 当前遍历位置的
	Key() []byte

	// Value 当前遍历位置的 Value 数据
	Value() *disk.LogRecordPos

	// Close 关闭迭代器，释放相应资源
	Close()
}
