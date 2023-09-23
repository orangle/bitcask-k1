package index

import (
	"bytes"
	"github.com/google/btree"
	"github.com/orangle/bitcask-k1/disk"
	"sort"
	"sync"
)

type Btree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func (bt *Btree) Put(key []byte, pos *disk.LogRecordPos) *disk.LogRecordPos {
	item := &Item{key, pos}
	bt.lock.Lock()
	oldItem := bt.tree.ReplaceOrInsert(item)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil
	}
	// kv set多次，旧的值需要标记？
	return oldItem.(*Item).pos
}

func (bt *Btree) Get(key []byte) *disk.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *Btree) Delete(key []byte) (*disk.LogRecordPos, bool) {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*Item).pos, true
}

func (bt *Btree) Size() int {
	return bt.tree.Len()
}

func (bt *Btree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBtreeIterator(bt.tree, reverse)
}

func (bt *Btree) Close() error {
	return nil
}

// BtreeIterator btree 迭代器(对整个内存索引的操作)
type BtreeIterator struct {
	currIndex int     // 当前的位置
	reverse   bool    // 遍历的方向
	values    []*Item // key+位置索引信息
}

func newBtreeIterator(tree *btree.BTree, reverse bool) *BtreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	// type ItemIteratorG[T any] func(item T) bool
	// 需要传入一个方法
	allValues := func(item btree.Item) bool {
		values[idx] = item.(*Item)
		idx++
		return true
	}

	if reverse {
		tree.Descend(allValues)
	} else {
		tree.Ascend(allValues)
	}

	return &BtreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (bi *BtreeIterator) Rewind() {
	bi.currIndex = 0
}

func (bi *BtreeIterator) Seek(key []byte) {
	if bi.reverse {
		// Todo 用途?
		bi.currIndex = sort.Search(len(bi.values), func(i int) bool {
			return bytes.Compare(bi.values[i].key, key) <= 0
		})
		return
	}

	bi.currIndex = sort.Search(len(bi.values), func(i int) bool {
		return bytes.Compare(bi.values[i].key, key) >= 0
	})
}

func (bi *BtreeIterator) Next() {
	bi.currIndex += 1
}

func (bi *BtreeIterator) Valid() bool {
	return bi.currIndex < len(bi.values)
}

func (bi *BtreeIterator) Key() []byte {
	return bi.values[bi.currIndex].key
}

func (bi *BtreeIterator) Value() *disk.LogRecordPos {
	return bi.values[bi.currIndex].pos
}

func (bi *BtreeIterator) Close() {
	bi.values = nil
}
