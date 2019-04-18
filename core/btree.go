package core

import (
	"fmt"
	"sort"
)

type Node interface {
	ID() uint32
	SetID(id uint32)
	Keys() []uint32
	Children() []uint32
	String() string
	NodeType() string
}

type InternalNode struct {
	id       uint32
	keys     []uint32
	children []uint32
}

func (n *InternalNode) ID() uint32 {
	return n.id
}

func (n *InternalNode) SetID(id uint32) {
	n.id = id
}

func (n *InternalNode) Keys() []uint32 {
	return n.keys
}

func (n *InternalNode) Children() []uint32 {
	return n.children
}

func (n *InternalNode) NodeType() string {
	return "InternalNode"
}

func (n *InternalNode) String() string {
	message := "InternalNode ("
	keys := n.Keys()
	for _, key := range keys {
		message = message + fmt.Sprintf("%v ", key)
	}
	message = message + ")"
	return message
}

type LeafNode struct {
	id     uint32
	tuples []*Tuple
}

func (n *LeafNode) ID() uint32 {
	return n.id
}

func (n *LeafNode) SetID(id uint32) {
	n.id = id
}

func (n *LeafNode) Children() []uint32 {
	return []uint32{}
}

func (n *LeafNode) Keys() []uint32 {
	keys := []uint32{}
	for _, tuple := range n.tuples {
		keys = append(keys, tuple.key)
	}
	return keys
}

func (n *LeafNode) NodeType() string {
	return "LeafNode"
}

func (n *LeafNode) String() string {
	message := "LeafNode ("
	keys := n.Keys()
	for _, key := range keys {
		message = message + fmt.Sprintf("%v ", key)
	}
	message = message + ")"
	return message
}

func (n *LeafNode) SetTuples(newTuples []*Tuple) {
	n.tuples = newTuples
}

type Tuple struct {
	key   uint32
	value []byte
}

type ByKey []*Tuple

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].key < a[j].key }

type BTree struct {
	rootNode            Node
	capacityPerLeafNode int
	noder               Noder
}

type Noder interface {
	Read(nodeId uint32) Node
	Add(node Node) uint32
}

func (t *BTree) String() string {
	message := ""
	nodes := []Node{}
	node := t.rootNode
	for node != nil {
		message = message + node.String() + "\n"
		for _, nodeId := range node.Children() {
			nodes = append(nodes, t.getNode(nodeId))
		}
		if len(nodes) > 0 {
			node, nodes = nodes[0], nodes[1:]
		} else {
			node = nil
		}
	}
	return message
}

func (t *BTree) getNode(nodeId uint32) Node {
	return t.noder.Read(nodeId)
}

func (t *BTree) addNewNode(node Node) uint32 {
	return t.noder.Add(node)
}

func (t *BTree) newRoot(middleKey uint32, children []uint32) {
	newRoot := &InternalNode{
		keys:     []uint32{middleKey},
		children: children,
	}
	t.addNewNode(newRoot)
	t.rootNode = newRoot
}

// Find correct leaf L.
// Put data entry into L in sorted order.
// If L has enough space done!
// Else, must split L into L and a new node L2
//  Redistribute entries evenly, copy up middle key
//  Insert index entry pointing to L2 into parent of L
func (t *BTree) Insert(key uint32, value []byte) {
	nodes := t.lookup([]Node{t.rootNode}, key)
	node, nodes := nodes[len(nodes)-1], nodes[:len(nodes)-1]
	leafNode := node.(*LeafNode)
	keys := node.Keys()
	newTuples := append(leafNode.tuples, &Tuple{key, value})
	sort.Sort(ByKey(newTuples))
	if len(keys) < t.capacityPerLeafNode {
		leafNode.SetTuples(newTuples)
	} else {
		midIdx := len(newTuples) / 2
		leafNode.SetTuples(newTuples[0:midIdx])
		leafNode2 := &LeafNode{tuples: newTuples[midIdx:]}
		nodeId := t.addNewNode(leafNode2)
		middleKey := newTuples[midIdx].key
		if len(nodes) == 0 {
			t.newRoot(middleKey, []uint32{leafNode.id, nodeId})
		} else {
			for i := len(nodes); i > 0; i-- {
				parentNode := nodes[i-1].(*InternalNode)
				result := parentNode.addChildren(middleKey, nodeId, t)
				if result.splited == false {
					break
				} else {
					middleKey = result.middleKey
					nodeId = result.newNodeId
					if t.rootNode == parentNode {
						t.newRoot(middleKey, []uint32{parentNode.id, nodeId})
					}
				}
			}
		}

	}
}

func (n *InternalNode) addChildren(key uint32, nodeId uint32, t *BTree) AddKeyResult {
	idx := 0
	for _, k := range n.keys {
		if key < k {
			break
		}
		idx++
	}

	keys := append(n.keys, 0)
	copy(keys[idx+1:], keys[idx:])
	keys[idx] = key

	children := append(n.children, 0)
	copy(children[idx+2:], children[idx+1:])
	children[idx+1] = nodeId

	if len(keys) <= t.capacityPerLeafNode {
		n.keys = keys
		n.children = children
		return AddKeyResult{
			splited: false,
		}
	} else {
		midIdx := len(keys) / 2
		n.keys = keys[0:midIdx]
		n.children = children[0 : midIdx+1]
		node2 := &InternalNode{
			keys:     keys[midIdx+1:],
			children: children[midIdx+1:],
		}
		middleKey := keys[midIdx]
		newNodeId := t.addNewNode(node2)
		return AddKeyResult{
			splited:   true,
			middleKey: middleKey,
			newNodeId: newNodeId,
		}
	}
}

type AddKeyResult struct {
	splited   bool
	middleKey uint32
	newNodeId uint32
}

func (t *BTree) Delete(key uint32) {

}

func (t *BTree) Find(key uint32) *Tuple {
	nodes := t.lookup([]Node{t.rootNode}, key)
	leafNode := nodes[len(nodes)-1].(*LeafNode)
	for _, tuple := range leafNode.tuples {
		if tuple.key == key {
			return tuple
		}
	}
	return nil
}

func (t *BTree) First() *Tuple {
	node := t.rootNode
	for node.NodeType() != "LeafNode" {
		leftChild := node.Children()[0]
		node = t.getNode(leftChild)
	}
	leafNode := node.(*LeafNode)
	return leafNode.tuples[0]
}

// Return left most leaf node
func (t *BTree) FirstLeafNode() *LeafNode {
	node := t.rootNode
	for node.NodeType() != "LeafNode" {
		leftChild := node.Children()[0]
		node = t.getNode(leftChild)
	}
	leafNode := node.(*LeafNode)
	return leafNode
}

// Return node's right sibling
func (t *BTree) NextLeafNode(node *LeafNode) *LeafNode {
	key := node.Keys()[len(node.Keys())-1]
	nodes := t.lookup([]Node{t.rootNode}, key+1)
	leafNode := nodes[len(nodes)-1].(*LeafNode)
	return leafNode
}

// return node and it's ancestor nodes
func (t *BTree) lookup(nodes []Node, key uint32) []Node {
	node := nodes[len(nodes)-1]

	if len(node.Children()) == 0 {
		return nodes
	}

	idx := 0
	for _, k := range node.Keys() {
		if key < k {
			break
		}
		idx++
	}
	nodeId := node.Children()[idx]
	newNodes := append(nodes, t.getNode(nodeId))
	return t.lookup(newNodes, key)
}
