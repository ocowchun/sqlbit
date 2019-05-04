package core

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

func (n *DummyNoder) NewLeafNode(tuples []*Tuple) *LeafNode {
	bs := emptyPage()
	node := &LeafNode{
		tuples: tuples,
		bytes:  &bs,
	}
	id := n.add(node)
	node.SetID(id)
	return node
}

func (n *DummyNoder) NewInternalNode(keys []uint32, children []uint32) *InternalNode {
	bs := emptyPage()
	node := &InternalNode{
		keys:     keys,
		children: children,
		bytes:    &bs,
	}

	id := n.add(node)
	node.SetID(id)
	return node
}

func (n *DummyNoder) Clean(nodeId uint32, isDirty bool) {
}

func NewDummyTree() *BTree {
	rootNode := &LeafNode{
		id:     0,
		tuples: []*Tuple{},
	}
	return &BTree{
		rootNode:            rootNode,
		capacityPerLeafNode: 2,
	}
}
