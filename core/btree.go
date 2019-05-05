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
	bytes    *Page
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

func (n *InternalNode) Update(keys []uint32, children []uint32) {
	n.keys = keys
	n.children = children
	n.syncBytes()
}

const INTERNAL_NODE_NUM_KEYS_OFFSET = PAGE_TYPE_SIZE
const INTERNAL_NODE_FIRST_CHILD_OFFSET = INTERNAL_NODE_HEADER_SIZE

func (n *InternalNode) syncBytes() {
	numKeys := uint32(len(n.keys))
	copy(n.bytes[INTERNAL_NODE_NUM_KEYS_OFFSET:INTERNAL_NODE_NUM_KEYS_OFFSET+INTERNAL_NODE_NUM_KEYS_SIZE], convertUint32ToBytes(numKeys))
	if len(n.children) > 0 {
		offset := INTERNAL_NODE_FIRST_CHILD_OFFSET
		end := INTERNAL_NODE_FIRST_CHILD_OFFSET + INTERNAL_NODE_CHILD_SIZE
		copy(n.bytes[offset:end], convertUint32ToBytes(n.children[0]))
		for idx, key := range n.keys {
			offset = end
			end = end + INTERNAL_NODE_KEY_SIZE
			copy(n.bytes[offset:end], convertUint32ToBytes(key))

			offset = end
			end = end + INTERNAL_NODE_CHILD_SIZE
			copy(n.bytes[offset:end], convertUint32ToBytes(n.children[idx+1]))
		}
	}
}

type LeafNode struct {
	id     uint32
	tuples []*Tuple
	bytes  *Page
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

func (n *LeafNode) Tuples() []*Tuple {
	return n.tuples
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
	n.syncBytes()
}

const LEAF_NODE_NUM_TUPLES_OFFSET = PAGE_TYPE_SIZE
const LEAF_NODE_FIRST_CHILD_OFFSET = LEAF_NODE_HEADER_SIZE

func (n *LeafNode) syncBytes() {
	numTuples := uint32(len(n.tuples))
	copy(n.bytes[LEAF_NODE_NUM_TUPLES_OFFSET:LEAF_NODE_NUM_TUPLES_OFFSET+LEAF_NODE_NUM_TUPLE_SIZE], convertUint32ToBytes(numTuples))

	offset := LEAF_NODE_FIRST_CHILD_OFFSET
	end := LEAF_NODE_FIRST_CHILD_OFFSET
	for _, tuple := range n.tuples {
		offset = end
		end = end + ROW_SIZE
		copy(n.bytes[offset:end], tuple.value)
	}
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
	// Deprecated
	rootNode            Node
	rootNodeID          uint32
	capacityPerLeafNode int
}

type Noder interface {
	Read(nodeId uint32) Node
	NewLeafNode(tuples []*Tuple) *LeafNode
	NewInternalNode(keys []uint32, children []uint32) *InternalNode
}

func (t *BTree) RootNode(noder Noder) Node {
	return noder.Read(t.rootNodeID)
}

func (t *BTree) String(noder Noder) string {
	message := ""
	nodes := []Node{}
	node := t.RootNode(noder)
	for node != nil {
		message = message + node.String() + "\n"
		for _, nodeId := range node.Children() {
			nodes = append(nodes, t.getNode(nodeId, noder))
		}
		if len(nodes) > 0 {
			node, nodes = nodes[0], nodes[1:]
		} else {
			node = nil
		}
	}
	return message
}

func (t *BTree) getNode(nodeId uint32, noder Noder) Node {
	return noder.Read(nodeId)
}

func (t *BTree) newRoot(middleKey uint32, children []uint32, noder Noder) {
	newRoot := noder.NewInternalNode([]uint32{middleKey}, children)
	t.rootNode = newRoot
	t.rootNodeID = newRoot.ID()
}

// Find correct leaf L.
// Put data entry into L in sorted order.
// If L has enough space done!
// Else, must split L into L and a new node L2
//  Redistribute entries evenly, copy up middle key
//  Insert index entry pointing to L2 into parent of L
func (t *BTree) Insert(key uint32, value []byte, noder Noder) {
	rootNode := noder.Read(t.rootNodeID)
	nodes := t.lookup([]Node{rootNode}, key, noder)
	leafNode := nodes[len(nodes)-1].(*LeafNode)
	nodes = nodes[:len(nodes)-1]
	keys := leafNode.Keys()
	newTuples := append(leafNode.Tuples(), &Tuple{key, value})
	sort.Sort(ByKey(newTuples))

	if len(keys) < t.capacityPerLeafNode {
		leafNode.SetTuples(newTuples)
	} else {
		midIdx := len(newTuples) / 2
		leafNode.SetTuples(newTuples[0:midIdx])
		leafNode2 := noder.NewLeafNode(newTuples[midIdx:])
		nodeID := leafNode2.ID()
		middleKey := newTuples[midIdx].key

		if len(nodes) == 0 {
			t.newRoot(middleKey, []uint32{leafNode.ID(), nodeID}, noder)

		} else {
			for i := len(nodes); i > 0; i-- {
				parentNode := nodes[i-1].(*InternalNode)
				result := t.addChildrenToInternalNode(middleKey, nodeID, parentNode, noder)
				if result.splited == false {
					break
				} else {
					middleKey = result.middleKey
					nodeID = result.newNodeId
					if t.RootNode(noder) == parentNode {
						t.newRoot(middleKey, []uint32{parentNode.id, nodeID}, noder)
					}
				}
			}
		}
	}

}

func (t *BTree) addChildrenToInternalNode(key uint32, nodeID uint32, internalNode *InternalNode, noder Noder) AddKeyResult {
	idx := 0
	for _, k := range internalNode.keys {
		if key < k {
			break
		}
		idx++
	}

	keys := append(internalNode.keys, 0)
	copy(keys[idx+1:], keys[idx:])
	keys[idx] = key

	children := append(internalNode.children, 0)
	copy(children[idx+2:], children[idx+1:])
	children[idx+1] = nodeID

	if len(keys) <= t.capacityPerLeafNode {
		internalNode.Update(keys, children)
		return AddKeyResult{
			splited: false,
		}
	} else {
		midIdx := len(keys) / 2
		internalNode.Update(keys[0:midIdx], children[0:midIdx+1])
		node2 := noder.NewInternalNode(keys[midIdx+1:], children[midIdx+1:])
		middleKey := keys[midIdx]
		return AddKeyResult{
			splited:   true,
			middleKey: middleKey,
			newNodeId: node2.ID(),
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

func (t *BTree) Find(key uint32, noder Noder) *Tuple {
	rootNode := t.RootNode(noder)
	nodes := t.lookup([]Node{rootNode}, key, noder)
	leafNode := nodes[len(nodes)-1].(*LeafNode)
	for _, tuple := range leafNode.Tuples() {
		if tuple.key == key {
			return tuple
		}
	}
	return nil
}

func (t *BTree) First(noder Noder) *Tuple {
	node := t.RootNode(noder)
	for node.NodeType() != "LeafNode" {
		leftChild := node.Children()[0]
		node = t.getNode(leftChild, noder)
	}
	leafNode := node.(*LeafNode)
	return leafNode.Tuples()[0]
}

// Return left most leaf node
func (t *BTree) FirstLeafNode(noder Noder) *LeafNode {
	node := t.RootNode(noder)
	for node.NodeType() != "LeafNode" {
		leftChild := node.Children()[0]
		node = t.getNode(leftChild, noder)
	}
	leafNode := node.(*LeafNode)
	return leafNode
}

// Return node's right sibling
func (t *BTree) NextLeafNode(node *LeafNode, noder Noder) *LeafNode {
	key := node.Keys()[len(node.Keys())-1]
	nodes := t.lookup([]Node{t.RootNode(noder)}, key+1, noder)
	leafNode := nodes[len(nodes)-1].(*LeafNode)
	return leafNode
}

// return node and it's ancestor nodes
func (t *BTree) lookup(nodes []Node, key uint32, noder Noder) []Node {
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
	newNodes := append(nodes, t.getNode(nodeId, noder))
	return t.lookup(newNodes, key, noder)
}
