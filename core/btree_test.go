package core

import (
	"fmt"
	"testing"
)

func TestBtree(t *testing.T) {
	rootNode := &LeafNode{
		id:     0,
		tuples: []*Tuple{},
	}
	tree := &BTree{
		rootNode:        rootNode,
		capacityPerNode: 2,
		nodes:           []Node{rootNode},
	}
	tree.Insert(1, "a")
	tree.Insert(2, "b")
	tree.Insert(3, "c")
	tree.Insert(4, "d")
	tree.Insert(5, "f")
	tree.Insert(6, "g")
	tree.Insert(7, "h")
	tree.Insert(8, "i")
	tree.Insert(9, "j")

	fmt.Println(tree)
}
