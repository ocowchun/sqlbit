package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadPage(t *testing.T) {
	replacer := &DummyReplacer{
		frameIndices: []uint32{},
		pinnedIdxMap: make(map[uint32]bool),
	}
	page0 := emptyPage()
	expectedPage := createPageFromSlice([]byte{1, 2, 3, 4, 5})
	pager := &DummyPager{body: append(page0[:], expectedPage[:]...)}
	bufferPool := NewBufferPool(replacer, pager, 5, 100)
	tx := NewTransaction(1, bufferPool)

	page, err := tx.ReadPage(uint32(1))

	assert.Nil(t, err)
	assert.Equal(t, expectedPage, *page)
}

func TestReadPageFromCache(t *testing.T) {
	replacer := &DummyReplacer{
		frameIndices: []uint32{},
		pinnedIdxMap: make(map[uint32]bool),
	}
	page0 := emptyPage()
	expectedPage := createPageFromSlice([]byte{1, 2, 3, 4, 5})
	pager := &DummyPager{body: append(page0[:], expectedPage[:]...)}
	bufferPool := NewBufferPool(replacer, pager, 5, 100)
	tx := NewTransaction(1, bufferPool)
	pageID := uint32(1)
	tx.ReadPage(pageID)

	page, err := tx.ReadPage(pageID)

	assert.Nil(t, err)
	assert.Equal(t, expectedPage, *page)
	assert.Equal(t, int32(1), bufferPool.pageTable[pageID].referenceCount)
}

func TestMarkAsDirty(t *testing.T) {
	replacer := &DummyReplacer{
		frameIndices: []uint32{},
		pinnedIdxMap: make(map[uint32]bool),
	}
	page0 := emptyPage()
	expectedPage := createPageFromSlice([]byte{1, 2, 3, 4, 5})
	pager := &DummyPager{body: append(page0[:], expectedPage[:]...)}
	bufferPool := NewBufferPool(replacer, pager, 5, 100)
	tx := NewTransaction(1, bufferPool)
	pageID := uint32(1)

	page, err := tx.ReadPage(pageID)
	page[0] = 100
	tx.MarkAsDirty(pageID)

	assert.Nil(t, err)
	assert.Equal(t, true, tx.pageTable[pageID].isDirty)
}

func TestTransactionCommit(t *testing.T) {
	replacer := &DummyReplacer{
		frameIndices: []uint32{},
		pinnedIdxMap: make(map[uint32]bool),
	}
	page0 := emptyPage()
	expectedPage := createPageFromSlice([]byte{1, 2, 3, 4, 5})
	pager := &DummyPager{body: append(page0[:], expectedPage[:]...)}
	bufferPool := NewBufferPool(replacer, pager, 5, 100)
	tx := NewTransaction(1, bufferPool)
	pageID := uint32(1)

	page, err := tx.ReadPage(pageID)
	page[0] = 100
	tx.MarkAsDirty(pageID)
	tx.Commit()

	assert.Nil(t, err)
	assert.Equal(t, int32(0), bufferPool.pageTable[pageID].referenceCount)
	frameIdx := bufferPool.pageTable[pageID].frameIdx
	assert.Equal(t, int8(100), int8(bufferPool.frames[frameIdx][0]))
}

func TestTransactionRollback(t *testing.T) {
	replacer := &DummyReplacer{
		frameIndices: []uint32{},
		pinnedIdxMap: make(map[uint32]bool),
	}
	page0 := emptyPage()
	expectedPage := createPageFromSlice([]byte{1, 2, 3, 4, 5})
	pager := &DummyPager{body: append(page0[:], expectedPage[:]...)}
	bufferPool := NewBufferPool(replacer, pager, 5, 100)
	tx := NewTransaction(1, bufferPool)
	pageID := uint32(1)

	page, err := tx.ReadPage(pageID)
	page[0] = 100
	tx.MarkAsDirty(pageID)
	tx.Rollback()

	assert.Nil(t, err)
	assert.Equal(t, int32(0), bufferPool.pageTable[pageID].referenceCount)
	frameIdx := bufferPool.pageTable[pageID].frameIdx
	assert.Equal(t, int8(1), int8(bufferPool.frames[frameIdx][0]))
}
