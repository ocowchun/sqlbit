package core

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTxNoderReadInternalNode(t *testing.T) {
	bs := emptyPage()
	page_type_bytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(page_type_bytes, uint16(PAGE_TYPE_INTERNAL_NODE))
	copy(bs[0:2], page_type_bytes)
	internalNode := &InternalNode{
		keys:     []uint32{},
		children: []uint32{},
		bytes:    &bs,
	}

	keys := []uint32{5}
	children := []uint32{4, 5}
	internalNode.Update(keys, children)
	page0 := emptyPage()
	pager := &DummyPager{body: append(page0[:], internalNode.bytes[:]...)}
	replacer := NewDummyReplacer()
	bufferPool := NewBufferPool(replacer, pager, 5, 100)
	tx := NewTransaction(1, bufferPool)
	noder := &TransactionNoder{transaction: tx}
	nodeID := uint32(1)

	node := noder.Read(nodeID)

	assert.Equal(t, "InternalNode", node.NodeType())
	assert.Equal(t, uint32(1), node.ID())
	assert.Equal(t, keys, node.Keys())
	assert.Equal(t, children, node.Children())
}

func TestTxNoderReadLeafNode(t *testing.T) {
	bs := emptyPage()
	page_type_bytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(page_type_bytes, uint16(PAGE_TYPE_LEAF_NODE))
	copy(bs[0:2], page_type_bytes)
	leafNode := &LeafNode{
		tuples: []*Tuple{},
		bytes:  &bs,
	}
	row1 := NewRow(1, "Harry", "harry@hogwarts.edu")
	tuple1 := &Tuple{row1.Id(), row1.Bytes()}
	tuples := []*Tuple{tuple1}
	leafNode.SetTuples(tuples)
	page0 := emptyPage()
	pager := &DummyPager{body: append(page0[:], leafNode.bytes[:]...)}
	replacer := NewDummyReplacer()
	bufferPool := NewBufferPool(replacer, pager, 5, 100)
	tx := NewTransaction(1, bufferPool)
	noder := &TransactionNoder{transaction: tx}
	nodeID := uint32(1)

	node := noder.Read(nodeID).(*LeafNode)

	assert.Equal(t, "LeafNode", node.NodeType())
	assert.Equal(t, uint32(1), node.ID())
	assert.Equal(t, tuples, node.tuples)
}

func TestTxNoderNewLeafNode(t *testing.T) {
	page0 := emptyPage()
	pager := &DummyPager{body: page0[:]}
	replacer := NewDummyReplacer()
	bufferPool := NewBufferPool(replacer, pager, 5, 100)
	tx := NewTransaction(1, bufferPool)
	noder := &TransactionNoder{transaction: tx}
	row1 := NewRow(1, "Harry", "harry@hogwarts.edu")
	tuple1 := &Tuple{row1.Id(), row1.Bytes()}
	tuples := []*Tuple{tuple1}

	node := noder.NewLeafNode(tuples)

	assert.Equal(t, uint32(1), node.ID())
	assert.Equal(t, tuples, node.tuples)
}

func TestTxNoderNewInternalNode(t *testing.T) {
	page0 := emptyPage()
	pager := &DummyPager{body: page0[:]}
	replacer := NewDummyReplacer()
	bufferPool := NewBufferPool(replacer, pager, 5, 100)
	tx := NewTransaction(1, bufferPool)
	noder := &TransactionNoder{transaction: tx}
	keys := []uint32{5}
	children := []uint32{4, 5}

	node := noder.NewInternalNode(keys, children)

	assert.Equal(t, uint32(1), node.ID())
	assert.Equal(t, keys, node.keys)
	assert.Equal(t, children, node.children)
}
