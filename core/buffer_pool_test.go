package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type DummyPager struct {
	body []byte
}

func (d *DummyPager) Read(offset int64, bs *PageBody) error {
	copy(bs[:], d.body[offset:offset+int64(PAGE_SIZE)])
	return nil
}

func (d *DummyPager) Write(offset int64, bs *PageBody) error {
	tmp := d.body
	d.body = append(tmp[:offset], append(bs[:], tmp[offset+PAGE_SIZE:]...)...)
	return nil
}

func (d *DummyPager) IncrementPageID() PageID {
	return PageID(len(d.body) / PAGE_SIZE)
}

func createPageFromSlice(slice []byte) PageBody {
	page := emptyPageBody()
	for i, b := range slice {
		page[i] = b
	}
	return page
}

func TestFetchPage(t *testing.T) {
	replacer := &DummyReplacer{
		frameIndices: []PageID{},
		pinnedIdxMap: make(map[PageID]bool),
	}
	bs := emptyPageBody()
	expectedPage := createPageFromSlice([]byte{1, 2, 3, 4, 5})
	pager := &DummyPager{body: append(bs[:], expectedPage[:]...)}
	pool := NewBufferPool(replacer, pager, 5, 100)
	pageID := PageID(1)

	page, err := pool.FetchPage(pageID)

	assert.Nil(t, err)
	assert.Equal(t, expectedPage, *page)
}

func TestFetchPageWhenNewFrame(t *testing.T) {
	replacer := &DummyReplacer{
		frameIndices: []PageID{},
		pinnedIdxMap: make(map[PageID]bool),
	}
	bs := emptyPageBody()
	expectedPage := createPageFromSlice([]byte{1, 2, 3, 4, 5})
	pager := &DummyPager{body: append(bs[:], expectedPage[:]...)}
	pool := NewBufferPool(replacer, pager, 0, 100)
	pageID := PageID(1)

	page, err := pool.FetchPage(pageID)

	assert.Nil(t, err)
	assert.Equal(t, PageBody(expectedPage), *page)
}

func TestFetchPageWithEvictPage(t *testing.T) {
	replacer := &DummyReplacer{
		frameIndices: []PageID{},
		pinnedIdxMap: make(map[PageID]bool),
	}
	bs := emptyPageBody()
	expectedPage := createPageFromSlice([]byte{1, 2, 3, 4, 5})
	pager := &DummyPager{body: append(bs[:], expectedPage[:]...)}
	pool := NewBufferPool(replacer, pager, 1, 1)
	pageID1 := PageID(0)
	pool.FetchPage(pageID1)
	pageID2 := PageID(1)

	pool.UnpinPage(pageID1, false)
	page, err := pool.FetchPage(pageID2)

	assert.Nil(t, err)
	assert.Equal(t, PageBody(expectedPage), *page)
}

func TestFetchPageWithEvictPageFailed(t *testing.T) {
	replacer := &DummyReplacer{
		frameIndices: []PageID{},
		pinnedIdxMap: make(map[PageID]bool),
	}
	bs := make([]byte, 4096)
	expectedPage := append([]byte{1, 2, 3, 4, 5}, make([]byte, PAGE_SIZE-5)...)
	pager := &DummyPager{body: append(bs, expectedPage...)}
	pool := NewBufferPool(replacer, pager, 1, 1)
	pageID1 := PageID(0)
	pool.FetchPage(pageID1)
	pageID2 := PageID(1)

	page, err := pool.FetchPage(pageID2)

	assert.Nil(t, page)
	assert.Equal(t, "no victim to evict", err.Error())
}

func TestFlushPage(t *testing.T) {
	replacer := &DummyReplacer{
		frameIndices: []PageID{},
		pinnedIdxMap: make(map[PageID]bool),
	}
	bs := make([]byte, 4096)
	pager := &DummyPager{body: bs}
	pool := NewBufferPool(replacer, pager, 1, 1)
	pageID := PageID(0)
	page, _ := pool.FetchPage(pageID)

	for i, b := range []byte{1, 2, 3, 4, 5} {
		page[i] = b
	}
	pool.UnpinPage(pageID, true)
	pool.FlushPage(pageID)

	assert.Equal(t, []byte{1, 2, 3, 4, 5}, pager.body[:5])
	assert.Equal(t, false, pool.pageTable[pageID].isDirty)
}
