package core

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

type DummyReplacer struct {
	frameIndices []uint32
	pinnedIdxMap map[uint32]bool
}

func (d *DummyReplacer) Insert(frameIdx uint32) {
	d.frameIndices = append(d.frameIndices, frameIdx)
}

func (d *DummyReplacer) Victim() (uint32, error) {
	if len(d.frameIndices) > 0 {
		frameIdx := d.frameIndices[0]
		d.frameIndices = d.frameIndices[1:]
		return frameIdx, nil
	}
	return 0, errors.New("no victim to evict")
}

func (d *DummyReplacer) Erase(frameIdx uint32) {
	d.pinnedIdxMap[frameIdx] = true
}

type DummyPager struct {
	body []byte
}

func (d *DummyPager) Read(offset int64, bs []byte) {
	copy(bs, d.body[offset:offset+int64(PAGE_SIZE)])
}

func (d *DummyPager) Write(offset int64, bs []byte) {
	tmp := d.body
	d.body = append(tmp[:offset], append(bs, tmp[offset+PAGE_SIZE:]...)...)
}

func (d *DummyPager) IncrementPageID() uint32 {
	return uint32(len(d.body) / PAGE_SIZE)
}

func TestFetchPage(t *testing.T) {
	replacer := &DummyReplacer{
		frameIndices: []uint32{},
		pinnedIdxMap: make(map[uint32]bool),
	}
	bs := make([]byte, 4096)
	expectedPage := append([]byte{1, 2, 3, 4, 5}, make([]byte, PAGE_SIZE-5)...)
	pager := &DummyPager{body: append(bs, expectedPage...)}
	pool := NewBufferPool(replacer, pager, 5, 100)
	pageID := uint32(1)

	page, err := pool.FetchPage(pageID)

	assert.Nil(t, err)
	assert.Equal(t, Page(expectedPage), page)
}

func TestFetchPageWhenNewFrame(t *testing.T) {
	replacer := &DummyReplacer{
		frameIndices: []uint32{},
		pinnedIdxMap: make(map[uint32]bool),
	}
	bs := make([]byte, 4096)
	expectedPage := append([]byte{1, 2, 3, 4, 5}, make([]byte, PAGE_SIZE-5)...)
	pager := &DummyPager{body: append(bs, expectedPage...)}
	pool := NewBufferPool(replacer, pager, 0, 100)
	pageID := uint32(1)

	page, err := pool.FetchPage(pageID)

	assert.Nil(t, err)
	assert.Equal(t, Page(expectedPage), page)
}

func TestFetchPageWithEvictPage(t *testing.T) {
	replacer := &DummyReplacer{
		frameIndices: []uint32{},
		pinnedIdxMap: make(map[uint32]bool),
	}
	bs := make([]byte, 4096)
	expectedPage := append([]byte{1, 2, 3, 4, 5}, make([]byte, PAGE_SIZE-5)...)
	pager := &DummyPager{body: append(bs, expectedPage...)}
	pool := NewBufferPool(replacer, pager, 1, 1)
	pageID1 := uint32(0)
	pool.FetchPage(pageID1)
	pageID2 := uint32(1)

	pool.UnpinPage(pageID1, false)
	page, err := pool.FetchPage(pageID2)

	assert.Nil(t, err)
	assert.Equal(t, Page(expectedPage), page)
}

func TestFetchPageWithEvictPageFailed(t *testing.T) {
	replacer := &DummyReplacer{
		frameIndices: []uint32{},
		pinnedIdxMap: make(map[uint32]bool),
	}
	bs := make([]byte, 4096)
	expectedPage := append([]byte{1, 2, 3, 4, 5}, make([]byte, PAGE_SIZE-5)...)
	pager := &DummyPager{body: append(bs, expectedPage...)}
	pool := NewBufferPool(replacer, pager, 1, 1)
	pageID1 := uint32(0)
	pool.FetchPage(pageID1)
	pageID2 := uint32(1)

	page, err := pool.FetchPage(pageID2)

	assert.Nil(t, page)
	assert.Equal(t, "no victim to evict", err.Error())
}

func TestFlushPage(t *testing.T) {
	replacer := &DummyReplacer{
		frameIndices: []uint32{},
		pinnedIdxMap: make(map[uint32]bool),
	}
	bs := make([]byte, 4096)
	pager := &DummyPager{body: bs}
	pool := NewBufferPool(replacer, pager, 1, 1)
	pageID := uint32(0)
	page, _ := pool.FetchPage(pageID)

	expectedPage := append([]byte{1, 2, 3, 4, 5}, make([]byte, PAGE_SIZE-5)...)
	copy(page, expectedPage)
	pool.UnpinPage(pageID, true)
	pool.FlushPage(pageID)

	assert.Equal(t, expectedPage, pager.body[:PAGE_SIZE])
	assert.Equal(t, false, pool.pageTable[pageID].isDirty)
}
