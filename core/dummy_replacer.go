package core

import "errors"

type DummyReplacer struct {
	frameIndices []uint32
	pinnedIdxMap map[uint32]bool
}

func NewDummyReplacer() *DummyReplacer {
	return &DummyReplacer{
		frameIndices: []uint32{},
		pinnedIdxMap: make(map[uint32]bool),
	}
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
