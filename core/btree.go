package core

import (
	"fmt"
	"sort"
)

type BTree struct {
	rootNode        Node
	capacityPerNode int
	nodes           []Node
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

type Node interface {
	SetID(id int)
	Keys() []int
	Children() []int
	String() string
	// perim() float64
}

type InternalNode struct {
	id   int
	keys []int
	// values   []string
	// tuples   []*Tuple
	children []int
}

func (n *InternalNode) SetID(id int) {
	n.id = id
}

func (n *InternalNode) Keys() []int {
	return n.keys
}

func (n *InternalNode) Children() []int {
	return n.children
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
	id     int
	tuples []*Tuple
}

func (n *LeafNode) SetID(id int) {
	n.id = id
}

func (n *LeafNode) Children() []int {
	return []int{}
}

// func NewNode(tuples []*Tuple, children []int) *LeafNode {
// 	return &LeafNode{tuples: tuples, children: children}
// }

func (n *LeafNode) Keys() []int {
	keys := []int{}
	for _, tuple := range n.tuples {
		keys = append(keys, tuple.key)
	}
	return keys
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

type Tuple struct {
	key   int
	value string
}

type ByKey []*Tuple

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].key < a[j].key }

func (t *BTree) addNewNode(node Node) int {
	idx := len(t.nodes)
	t.nodes = append(t.nodes, node)
	node.SetID(idx)
	return idx
}

func (t *BTree) newRoot(middleKey int, children []int) {
	newRoot := &InternalNode{
		keys:     []int{middleKey},
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
func (t *BTree) Insert(key int, value string) {
	nodes := t.lookup([]Node{t.rootNode}, key)
	node, nodes := nodes[len(nodes)-1], nodes[:len(nodes)-1]
	leafNode := node.(*LeafNode)
	keys := node.Keys()
	newTuples := append(leafNode.tuples, &Tuple{key, value})
	sort.Sort(ByKey(newTuples))
	if len(keys) < t.capacityPerNode {
		leafNode.tuples = newTuples
	} else {
		midIdx := len(newTuples) / 2
		leafNode.tuples = newTuples[0:midIdx]
		leafNode2 := &LeafNode{tuples: newTuples[midIdx:]}
		nodeId := t.addNewNode(leafNode2)
		middleKey := newTuples[midIdx].key
		if len(nodes) == 0 {
			t.newRoot(middleKey, []int{leafNode.id, nodeId})
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
						t.newRoot(middleKey, []int{parentNode.id, nodeId})
					}
				}
			}
		}

	}
}

// write a method: node add key
func (n *InternalNode) addChildren(key int, nodeId int, t *BTree) AddKeyResult {
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

	if len(keys) <= t.capacityPerNode {
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
	middleKey int
	newNodeId int
}

func (t *BTree) Delete(key int) {

}

func (t *BTree) getNode(nodeId int) Node {
	return t.nodes[nodeId]
}

// return node and it's ancestor nodes
func (t *BTree) lookup(nodes []Node, key int) []Node {
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
