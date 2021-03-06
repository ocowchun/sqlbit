package core

import (
	"errors"
	"sync"
	"sync/atomic"
)

type PageBody [PAGE_SIZE]byte

type Page struct {
	id      uint32
	body    *PageBody
	isDirty bool
}

func EmptyPage() *Page {
	body := emptyPageBody()
	return &Page{
		id:      uint32(0),
		body:    &body,
		isDirty: false,
	}
}

func (s *Page) MarkAsDirty() {
	s.isDirty = true
}

type Pager interface {
	Read(offset int64, page *PageBody) error
	Write(offset int64, page *PageBody) error
	IncrementPageID() uint32
}

type Replacer interface {
	Insert(pageID uint32)
	Victim() (uint32, error)
	Erase(pageID uint32)
}

type pageMeta struct {
	frameIdx       int
	isDirty        bool
	mu             sync.RWMutex
	referenceCount int32
	pageID         uint32
}

func newPageMeta(frameIdx int, pageID uint32) *pageMeta {
	return &pageMeta{
		frameIdx: frameIdx,
		isDirty:  false,
		pageID:   pageID,
	}
}

type BufferPool struct {
	pageTable        map[uint32]*pageMeta
	replacer         Replacer
	freeFrameIndices chan int
	frames           []*PageBody
	pager            Pager
	maxPageNum       int
	lock             sync.Mutex
}

func NewBufferPool(replacer Replacer, pager Pager, initPageNum, maxPageNum int) *BufferPool {
	freeFrameIndices := make(chan int, maxPageNum)

	b := &BufferPool{
		pageTable:        make(map[uint32]*pageMeta),
		replacer:         replacer,
		freeFrameIndices: freeFrameIndices,
		frames:           []*PageBody{},
		pager:            pager,
		maxPageNum:       maxPageNum,
	}

	for i := 0; i < initPageNum; i++ {
		bs := emptyPageBody()
		b.frames = append(b.frames, &bs)
		b.freeFrameIndices <- i
	}

	return b
}

func emptyPageBody() PageBody {
	return [PAGE_SIZE]byte{}
}

func (b *BufferPool) FetchPage(pageID uint32) (*PageBody, error) {
	if b.pageTable[pageID] != nil {
		meta := b.pageTable[pageID]
		meta.mu.RLock()
		if meta.pageID == pageID {
			atomic.AddInt32(&meta.referenceCount, 1)
			b.replacer.Erase(pageID)
			return b.frames[meta.frameIdx], nil
		} else {
			meta.mu.RUnlock()
			return nil, errors.New("meta.pageID != pageID")
		}
	}

	frameIdx, err := b.getFreeFrameIdx()
	if err != nil {
		return nil, err
	}

	meta := newPageMeta(frameIdx, pageID)
	b.pageTable[pageID] = meta

	meta.mu.RLock()
	atomic.AddInt32(&meta.referenceCount, 1)

	b.replacer.Erase(pageID)

	frame := b.frames[meta.frameIdx]
	b.pager.Read(int64(pageID)*int64(PAGE_SIZE), frame)
	return frame, nil
}

func (b *BufferPool) getFreeFrameIdx() (int, error) {
	select {
	case frameIdx := <-b.freeFrameIndices:
		return frameIdx, nil
	default:
		if len(b.frames) >= b.maxPageNum {
			pageId, err := b.replacer.Victim()
			if err != nil {
				return 0, err
			}
			frameIdx := b.pageTable[pageId].frameIdx
			b.evict(pageId)
			return frameIdx, nil
		}
		b.lock.Lock()
		defer b.lock.Unlock()
		bs := emptyPageBody()
		frameIdx := len(b.frames)
		b.frames = append(b.frames, &bs)
		return frameIdx, nil
	}
}

// lock page before evict it
func (b *BufferPool) evict(pageId uint32) {
	meta := b.pageTable[pageId]
	meta.mu.Lock()
	delete(b.pageTable, pageId)
	meta.mu.Unlock()
}

func (b *BufferPool) UnpinPage(pageID uint32, isDirty bool) {
	meta := b.pageTable[pageID]
	if isDirty {
		meta.isDirty = true
	}
	meta.mu.RUnlock()
	referenceCount := atomic.AddInt32(&meta.referenceCount, -1)
	if referenceCount == 0 {
		b.replacer.Insert(pageID)
	}
}

type PageWithPageID struct {
	page   *PageBody
	pageID uint32
}

func (b BufferPool) NewPage() (*Page, error) {
	pageID := b.pager.IncrementPageID()
	frameIdx, err := b.getFreeFrameIdx()
	if err != nil {
		return nil, err
	}

	meta := newPageMeta(frameIdx, pageID)
	b.pageTable[pageID] = meta

	meta.mu.RLock()
	atomic.AddInt32(&meta.referenceCount, 1)

	b.replacer.Erase(pageID)

	bs := emptyPageBody()
	b.frames[meta.frameIdx] = &bs
	result := &Page{
		id:      pageID,
		body:    b.frames[meta.frameIdx],
		isDirty: false,
	}

	return result, nil
}

// What if flush and unpin page concurrently
func (b BufferPool) FlushPage(pageID uint32) {
	meta := b.pageTable[pageID]
	frame := b.frames[meta.frameIdx]
	b.pager.Write(int64(pageID)*int64(PAGE_SIZE), frame)
	meta.isDirty = false
}

func (b BufferPool) FlushAllPage() {
	for pageID, meta := range b.pageTable {
		frame := b.frames[meta.frameIdx]
		if meta.isDirty {
			b.pager.Write(int64(pageID)*int64(PAGE_SIZE), frame)
			meta.isDirty = false
		}
	}
}
