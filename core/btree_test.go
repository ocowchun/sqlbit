package core

import (
	"bufio"
	"encoding/binary"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

type DummyNoder struct {
	nodes []Node
}

func (n *DummyNoder) Read(nodeId uint32) Node {
	return n.nodes[nodeId]
}

func (n *DummyNoder) add(node Node) uint32 {
	idx := uint32(len(n.nodes))
	n.nodes = append(n.nodes, node)
	node.SetID(idx)
	return idx
}

func (n *DummyNoder) NewLeafNode(tuples []*Tuple) ILeafNode {
	node := &LeafNode{tuples: tuples}
	id := n.add(node)
	node.SetID(id)
	return node
}

func (n *DummyNoder) NewInternalNode(keys []uint32, children []uint32) *InternalNode {
	node := &InternalNode{
		keys:     keys,
		children: children,
	}

	id := n.add(node)
	node.SetID(id)
	return node
}
func TestBtree(t *testing.T) {
	rootNode := &LeafNode{
		id:     0,
		tuples: []*Tuple{},
	}
	tree := &BTree{
		rootNode:            rootNode,
		capacityPerLeafNode: 2,
		noder: &DummyNoder{
			nodes: []Node{rootNode},
		},
	}

	tree.Insert(1, []byte("a"))
	tree.Insert(2, []byte("b"))
	tree.Insert(3, []byte("c"))
	tree.Insert(4, []byte("d"))
	tree.Insert(5, []byte("f"))
	tree.Insert(6, []byte("g"))
	tree.Insert(7, []byte("h"))
	tree.Insert(8, []byte("i"))
	tree.Insert(9, []byte("j"))

	assert.Equal(t, tree.rootNode.Keys(), []uint32{5})
	assert.Equal(t, tree.getNode(tree.rootNode.Children()[0]).Keys(), []uint32{3})
	assert.Equal(t, tree.getNode(tree.rootNode.Children()[1]).Keys(), []uint32{7})
	for i := 0; i < 9; i++ {
		assert.NotNil(t, tree.Find(uint32(i+1)))
	}
	assert.Nil(t, tree.Find(10))
}

func prepareBtreeFile(fileName string, tuples []*Tuple) {
	f, _ := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	w := bufio.NewWriter(f)

	//Prepare Table Header
	bs := make([]byte, 2)
	binary.LittleEndian.PutUint16(bs, uint16(PAGE_TYPE_TABLE_HEADER))
	rootPageNum := uint32(1)
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, rootPageNum)
	bs = append(bs, b...)
	bs = append(bs, make([]byte, 4096-len(bs))...)

	//Prepare Leaf Node
	b = make([]byte, 2)
	binary.LittleEndian.PutUint16(b, uint16(PAGE_TYPE_LEAF_NODE))
	bs = append(bs, b...)
	numTuples := uint32(len(tuples))
	b = make([]byte, 4)
	binary.LittleEndian.PutUint32(b, numTuples)
	bs = append(bs, b...)
	for _, tuple := range tuples {
		bs = append(bs, tuple.value...)
	}

	w.Write(bs)
	w.Flush()
	f.Close()
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
		noder:               fileNoder,
	}

	assert.Equal(t, tree.rootNode.Keys(), []uint32{17, 42})
	removeTestFile()
}
