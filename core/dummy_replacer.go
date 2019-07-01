package core

import "errors"

type DummyReplacer struct {
	frameIndices []PageID
	pinnedIdxMap map[PageID]bool
}

func NewDummyReplacer() *DummyReplacer {
	return &DummyReplacer{
		frameIndices: []PageID{},
		pinnedIdxMap: make(map[PageID]bool),
	}
}

func (d *DummyReplacer) Insert(pageID PageID) {
	d.frameIndices = append(d.frameIndices, pageID)
}

func (d *DummyReplacer) Victim() (PageID, error) {
	if len(d.frameIndices) > 0 {
		frameIdx := d.frameIndices[0]
		d.frameIndices = d.frameIndices[1:]
		return frameIdx, nil
	}
	return 0, errors.New("no victim to evict")
}

func (d *DummyReplacer) Erase(pageID PageID) {
	d.pinnedIdxMap[pageID] = true
}
