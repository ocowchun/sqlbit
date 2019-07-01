package core

type DummyNoder struct {
	nodes []Node
}

func (n *DummyNoder) Read(nodeId PageID) Node {
	return n.nodes[nodeId]
}

func (n *DummyNoder) add(node Node) PageID {
	idx := PageID(len(n.nodes))
	n.nodes = append(n.nodes, node)
	node.SetID(idx)
	return idx
}

func (n *DummyNoder) NewLeafNode(tuples []*Tuple) *LeafNode {
	page := EmptyPage()
	node := &LeafNode{
		tuples:     append([]*Tuple(nil), tuples...),
		page:       page,
		nextNodeID: -1,
		prevNodeID: -1,
	}
	id := n.add(node)
	node.SetID(id)
	return node
}

func (n *DummyNoder) NewInternalNode(keys []uint32, children []PageID) *InternalNode {
	page := EmptyPage()
	node := &InternalNode{
		keys:     keys,
		children: children,
		page:     page,
	}

	id := n.add(node)
	node.SetID(id)
	return node
}

func (n *DummyNoder) Clean(nodeId uint32, isDirty bool) {
}

func NewDummyTree() *BTree {
	rootNode := &LeafNode{
		id:         0,
		tuples:     []*Tuple{},
		nextNodeID: -1,
		prevNodeID: -1,
	}
	return &BTree{
		rootNode:            rootNode,
		capacityPerLeafNode: 2,
	}
}
