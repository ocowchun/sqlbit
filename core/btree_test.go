package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInternalNodeUpdate(t *testing.T) {
	bs := emptyPage()
	node := &InternalNode{
		keys:     []uint32{},
		children: []uint32{},
		bytes:    &bs,
	}

	keys := []uint32{5}
	children := []uint32{4, 5}

	node.Update(keys, children)

	assert.Equal(t, node.keys, keys)
	assert.Equal(t, node.children, children)
	assert.Equal(t, bs[2:6], convertUint32ToBytes(uint32(len(keys))))
	assert.Equal(t, bs[6:10], convertUint32ToBytes(uint32(4)))
	assert.Equal(t, bs[10:14], convertUint32ToBytes(uint32(5)))
	assert.Equal(t, bs[14:18], convertUint32ToBytes(uint32(5)))
}

func TestLeafNodeUpdate(t *testing.T) {
	bs := emptyPage()
	node := &LeafNode{
		tuples: []*Tuple{},
		bytes:  &bs,
	}

	row1 := NewRow(1, "Harry", "harry@hogwarts.edu")
	tuple1 := &Tuple{row1.Id(), row1.Bytes()}
	tuples := []*Tuple{tuple1}

	node.SetTuples(tuples)

	assert.Equal(t, node.tuples, tuples)
	from := LEAF_NODE_FIRST_CHILD_OFFSET
	assert.Equal(t, bs[from:from+ROW_SIZE], tuple1.value)
}

func TestBtree(t *testing.T) {
	bs := emptyPage()
	rootNode := &LeafNode{
		id:     0,
		tuples: []*Tuple{},
		bytes:  &bs,
	}
	noder := &DummyNoder{
		nodes: []Node{rootNode},
	}
	tree := &BTree{
		rootNodeID:          0,
		capacityPerLeafNode: 2,
	}

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

func TestOpenBtreeFromFile(t *testing.T) {
	removeTestFile()
	fileName := getTestFileName()
	tuples := []*Tuple{createTuple(17), createTuple(42)}
	prepareBtreeFile(fileName, tuples)
	pager, _ := OpenPager2(fileName)
	fileNoder := NewFileNoder(pager)
	header, _ := fileNoder.ReadTableHeader()
	rootNode := fileNoder.Read(header.rootPageNum)

	tree := &BTree{
		rootNode:            rootNode,
		capacityPerLeafNode: ROW_PER_PAGE,
	}

	assert.Equal(t, tree.rootNode.Keys(), []uint32{17, 42})
	removeTestFile()
}
