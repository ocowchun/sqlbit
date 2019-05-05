package core

import (
	"errors"
)

type pageWithMeta struct {
	page     *Page
	snapshot Page
	isDirty  bool
}

type Transaction struct {
	id         int32
	pageTable  map[uint32]*pageWithMeta
	bufferPool *BufferPool
}

func NewTransaction(id int32, bufferPool *BufferPool) *Transaction {
	return &Transaction{
		id:         id,
		pageTable:  make(map[uint32]*pageWithMeta),
		bufferPool: bufferPool,
	}
}

// ID returns the transaction id.
func (t *Transaction) ID() int32 {
	return t.id
}

func (t *Transaction) ReadPage(pageID uint32) (*Page, error) {
	if t.pageTable[pageID] == nil {
		page, err := t.bufferPool.FetchPage(pageID)
		if err != nil {
			return nil, err
		}
		t.pageTable[pageID] = &pageWithMeta{
			page:     page,
			snapshot: *page,
			isDirty:  false,
		}
	}
	return &t.pageTable[pageID].snapshot, nil
}

func (t *Transaction) MarkAsDirty(pageID uint32) error {
	if t.pageTable[pageID] == nil {
		return errors.New("pageID not exists in pageTable")
	}
	t.pageTable[pageID].isDirty = true
	return nil
}

func (t *Transaction) Commit() {
	for pageID, pm := range t.pageTable {
		if pm.isDirty {
			copy(t.pageTable[pageID].page[:], t.pageTable[pageID].snapshot[:])
		}
		t.bufferPool.UnpinPage(pageID, pm.isDirty)
	}
}

func (t *Transaction) Rollback() {
	for pageID, _ := range t.pageTable {
		t.bufferPool.UnpinPage(pageID, false)
	}
}
