package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type DummyNoder struct {
	nodes []Node
}

func (n *DummyNoder) Read(nodeId uint32) Node {
	return n.nodes[nodeId]
}

func (n *DummyNoder) Add(node Node) uint32 {
	idx := uint32(len(n.nodes))
	n.nodes = append(n.nodes, node)
	node.SetID(idx)
	return idx
}
func TestBtree(t *testing.T) {
	rootNode := &LeafNode{
		id:     0,
		tuples: []*Tuple{},
	}
	tree := &BTree{
		rootNode:        rootNode,
		capacityPerNode: 2,
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
