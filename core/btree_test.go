package core

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func createTuple(key uint32) *Tuple {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, key)
	username := fmt.Sprintf("user-%d", key)
	email := fmt.Sprintf("%s@test.com", username)
	row := NewRow(key, username, email)
	return &Tuple{
		key:   key,
		value: row.Bytes(),
	}
}

func TestInternalNodeUpdate(t *testing.T) {
	page := EmptyPage()
	node := &InternalNode{
		keys:     []uint32{},
		children: []uint32{},
		page:     page,
	}

	keys := []uint32{5}
	children := []uint32{4, 5}

	node.Update(keys, children)

	assert.Equal(t, keys, node.keys)
	assert.Equal(t, children, node.children)
	assert.Equal(t, true, node.page.isDirty)
	assert.Equal(t, convertUint32ToBytes(uint32(len(keys))), node.page.body[2:6])
	assert.Equal(t, convertUint32ToBytes(uint32(4)), node.page.body[6:10])
	assert.Equal(t, convertUint32ToBytes(uint32(5)), node.page.body[10:14])
	assert.Equal(t, convertUint32ToBytes(uint32(5)), node.page.body[14:18])
}

func TestLeafNodeUpdate(t *testing.T) {
	page := EmptyPage()
	node := &LeafNode{
		tuples: []*Tuple{},
		page:   page,
	}

	row1 := NewRow(1, "Harry", "harry@hogwarts.edu")
	tuple1 := &Tuple{row1.Id(), row1.Bytes()}
	tuples := []*Tuple{tuple1}
	prevNodeID := uint32(9)
	nextNodeID := uint32(42)

	node.Update(tuples, prevNodeID, nextNodeID)

	assert.Equal(t, tuples, node.tuples)
	assert.Equal(t, true, node.page.isDirty)
	from := LEAF_NODE_FIRST_CHILD_OFFSET
	assert.Equal(t, tuple1.value, node.page.body[from:from+ROW_SIZE])
	assert.Equal(t, prevNodeID, node.PrevNodeID())
	assert.Equal(t, nextNodeID, node.NextNodeID())
}

func createDummyBtree() (*BTree, *DummyNoder) {
	page := EmptyPage()
	rootNode := &LeafNode{
		id:     0,
		tuples: []*Tuple{},
		page:   page,
	}
	noder := &DummyNoder{
		nodes: []Node{rootNode},
	}
	tree := &BTree{
		rootNodeID:          0,
		capacityPerLeafNode: 2,
	}
	return tree, noder
}

func TestBtreeInsert(t *testing.T) {
	tree, noder := createDummyBtree()

	tree.Insert(1, []byte("a"), noder)
	tree.Insert(2, []byte("b"), noder)
	tree.Insert(3, []byte("c"), noder)
	tree.Insert(4, []byte("d"), noder)
	tree.Insert(5, []byte("f"), noder)
	tree.Insert(6, []byte("g"), noder)
	tree.Insert(7, []byte("h"), noder)
	tree.Insert(8, []byte("i"), noder)
	tree.Insert(9, []byte("j"), noder)

	newRootNode := tree.RootNode(noder)
	assert.Equal(t, newRootNode.Keys(), []uint32{5})
	assert.Equal(t, tree.getNode(newRootNode.Children()[0], noder).Keys(), []uint32{3})
	assert.Equal(t, tree.getNode(newRootNode.Children()[1], noder).Keys(), []uint32{7})
	for i := 0; i < 9; i++ {
		assert.NotNil(t, tree.Find(uint32(i+1), noder), noder)
	}
	assert.Nil(t, tree.Find(10, noder))
}

func TestBtreeInsertWithSibling(t *testing.T) {
	tree, noder := createDummyBtree()

	tree.Insert(1, []byte("a"), noder)
	tree.Insert(2, []byte("b"), noder)
	tree.Insert(3, []byte("c"), noder)
	tree.Insert(4, []byte("d"), noder)
	tree.Insert(5, []byte("f"), noder)

	leafNode := tree.FindLeafNode(3, noder)
	nextNode := noder.Read(leafNode.NextNodeID())
	assert.Equal(t, []uint32{4, 5}, nextNode.Keys())
	prevNode := noder.Read(leafNode.PrevNodeID())
	assert.Equal(t, []uint32{2}, prevNode.Keys())
}
func TestBtreeNextLeafNode(t *testing.T) {
	tree, noder := createDummyBtree()
	tree.Insert(1, []byte("a"), noder)
	tree.Insert(2, []byte("b"), noder)
	tree.Insert(3, []byte("c"), noder)
	tree.Insert(4, []byte("d"), noder)
	node := tree.FirstLeafNode(noder)

	leafNode := tree.NextLeafNode(node, noder)

	assert.Equal(t, leafNode.Keys(), []uint32{2})
}

func TestBtreeFindLeafNode(t *testing.T) {
	tree, noder := createDummyBtree()
	tree.Insert(1, []byte("a"), noder)
	tree.Insert(2, []byte("b"), noder)
	tree.Insert(3, []byte("c"), noder)
	tree.Insert(4, []byte("d"), noder)

	leafNode := tree.FindLeafNode(uint32(3), noder)

	assert.Equal(t, leafNode.Keys(), []uint32{3, 4})
}

func TestBtreePrevLeafNode(t *testing.T) {
	tree, noder := createDummyBtree()
	tree.Insert(1, []byte("a"), noder)
	tree.Insert(2, []byte("b"), noder)
	tree.Insert(3, []byte("c"), noder)
	tree.Insert(4, []byte("d"), noder)
	node := tree.FindLeafNode(uint32(3), noder)

	leafNode := tree.PrevLeafNode(node, noder)

	assert.Equal(t, leafNode.Keys(), []uint32{2})
}

func TestFindLeafNodeByCondition(t *testing.T) {
	tree, noder := createDummyBtree()
	tree.Insert(1, []byte("a"), noder)
	tree.Insert(2, []byte("b"), noder)
	tree.Insert(3, []byte("c"), noder)
	tree.Insert(4, []byte("d"), noder)
	tree.Insert(5, []byte("d"), noder)

	leafNode, idx := tree.FindLeafNodeByCondition(uint32(1), "=", noder)
	assert.Equal(t, uint32(1), leafNode.Keys()[idx])

	leafNode, idx = tree.FindLeafNodeByCondition(uint32(2), ">=", noder)
	assert.Equal(t, uint32(2), leafNode.Keys()[idx])

	leafNode, idx = tree.FindLeafNodeByCondition(uint32(3), "<=", noder)
	assert.Equal(t, uint32(3), leafNode.Keys()[idx])

	leafNode, idx = tree.FindLeafNodeByCondition(uint32(3), "<", noder)
	assert.Equal(t, uint32(2), leafNode.Keys()[idx])

	leafNode, idx = tree.FindLeafNodeByCondition(uint32(3), ">", noder)
	assert.Equal(t, uint32(4), leafNode.Keys()[idx])

	leafNode, idx = tree.FindLeafNodeByCondition(uint32(5), ">", noder)
	assert.Equal(t, -1, idx)

	leafNode, idx = tree.FindLeafNodeByCondition(uint32(1), "<", noder)
	assert.Equal(t, -1, idx)
}
