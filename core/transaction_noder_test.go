package core

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func getTestFileName() string {
	dir, _ := os.Getwd()
	return dir + "/test.db"
}

func removeTestFile() {
	fileName := getTestFileName()
	_, err := os.Stat(fileName)
	if err == nil {
		os.Remove(fileName)
	}
}

func TestMain(m *testing.M) {
	removeTestFile()
	retCode := m.Run()
	removeTestFile()
	os.Exit(retCode)
}

func TestTxNoderReadInternalNode(t *testing.T) {
	page := EmptyPage()
	page_type_bytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(page_type_bytes, uint16(PAGE_TYPE_INTERNAL_NODE))
	copy(page.body[0:2], page_type_bytes)
	internalNode := &InternalNode{
		keys:     []uint32{},
		children: []uint32{},
		page:     page,
	}

	keys := []uint32{5}
	children := []uint32{4, 5}
	internalNode.Update(keys, children)
	page0 := emptyPageBody()
	pager := &DummyPager{body: append(page0[:], internalNode.page.body[:]...)}
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
	page := EmptyPage()
	page_type_bytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(page_type_bytes, uint16(PAGE_TYPE_LEAF_NODE))
	copy(page.body[0:2], page_type_bytes)
	leafNode := &LeafNode{
		tuples: []*Tuple{},
		page:   page,
	}
	row1 := NewRow(1, "Harry", "harry@hogwarts.edu")
	tuple1 := &Tuple{row1.Id(), row1.Bytes()}
	tuples := []*Tuple{tuple1}
	leafNode.Update(tuples)
	page0 := emptyPageBody()
	pager := &DummyPager{body: append(page0[:], leafNode.page.body[:]...)}
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
	page0 := emptyPageBody()
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
	page0 := emptyPageBody()
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
