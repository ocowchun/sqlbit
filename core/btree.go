package core

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

type Node interface {
	ID() PageID
	SetID(id PageID)
	Keys() []uint32
	ChildrenCount() int
	String() string
	NodeType() string
}

type InternalNode struct {
	id       PageID
	page     *Page
	keys     []uint32
	children []PageID
}

const INTERNAL_NODE_NUM_KEYS_SIZE = 4
const INTERNAL_NODE_HEADER_SIZE = PAGE_TYPE_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE
const INTERNAL_NODE_CHILD_SIZE = 4
const INTERNAL_NODE_KEY_SIZE = 4
const INTERNAL_NODE_KEY_PER_PAGE = (PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE - INTERNAL_NODE_CHILD_SIZE) / (INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE)
const INTERNAL_NODE_NUM_KEYS_OFFSET = PAGE_TYPE_SIZE
const INTERNAL_NODE_FIRST_CHILD_OFFSET = INTERNAL_NODE_HEADER_SIZE

func (n *InternalNode) ID() PageID {
	return n.id
}

func (n *InternalNode) SetID(id PageID) {
	n.id = id
}

func (n *InternalNode) Keys() []uint32 {
	return n.keys
}

func (n *InternalNode) PageIDs() []PageID {
	return n.children
}

func (n *InternalNode) ChildrenCount() int {
	return len(n.PageIDs())
}

func (n *InternalNode) Contains(pageId PageID) bool {
	for _, pid := range n.children {
		if pid == pageId {
			return true
		}
	}
	return false
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

func (n *InternalNode) Update(keys []uint32, pageIDs []PageID) {
	n.keys = keys
	n.children = pageIDs
	n.page.MarkAsDirty()
	n.syncBytes()
}

func convertUint32ToBytes(num uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, num)
	return b
}

func convertPageIDToBytes(num PageID) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, num)
	return buf.Bytes()
}

func (n *InternalNode) syncBytes() {
	numKeys := uint32(len(n.keys))
	copy(n.page.body[INTERNAL_NODE_NUM_KEYS_OFFSET:INTERNAL_NODE_NUM_KEYS_OFFSET+INTERNAL_NODE_NUM_KEYS_SIZE], convertUint32ToBytes(numKeys))
	if len(n.children) > 0 {
		offset := INTERNAL_NODE_FIRST_CHILD_OFFSET
		end := INTERNAL_NODE_FIRST_CHILD_OFFSET + INTERNAL_NODE_CHILD_SIZE
		copy(n.page.body[offset:end], convertPageIDToBytes(n.children[0]))
		for idx, key := range n.keys {
			offset = end
			end = end + INTERNAL_NODE_KEY_SIZE
			copy(n.page.body[offset:end], convertUint32ToBytes(key))

			offset = end
			end = end + INTERNAL_NODE_CHILD_SIZE
			copy(n.page.body[offset:end], convertPageIDToBytes(n.children[idx+1]))
		}
	}
}

type LeafNode struct {
	id         PageID
	nextNodeID PageID
	prevNodeID PageID
	tuples     []*Tuple
	page       *Page
}

const LEAF_NODE_NUM_TUPLES_OFFSET = PAGE_TYPE_SIZE
const LEAF_NODE_NUM_TUPLE_SIZE = 4

const LEAF_NODE_PREV_NODE_ID_OFFSET = LEAF_NODE_NUM_TUPLES_OFFSET + LEAF_NODE_NUM_TUPLE_SIZE
const LEAF_NODE_PREV_NODE_ID_SIZE = 4

const LEAF_NODE_NEXT_NODE_ID_OFFSET = LEAF_NODE_PREV_NODE_ID_OFFSET + LEAF_NODE_PREV_NODE_ID_SIZE
const LEAF_NODE_NEXT_NODE_ID_SIZE = 4

const LEAF_NODE_HEADER_SIZE = LEAF_NODE_NEXT_NODE_ID_OFFSET + LEAF_NODE_NEXT_NODE_ID_SIZE

const LEAF_NODE_FIRST_CHILD_OFFSET = LEAF_NODE_HEADER_SIZE

const LEAF_NODE_CHILD_SIZE = ROW_SIZE
const LEAF_NODE_KEY_PER_PAGE = (PAGE_SIZE - LEAF_NODE_HEADER_SIZE) / LEAF_NODE_CHILD_SIZE

func (n *LeafNode) ID() PageID {
	return n.id
}

// TODO handle nil
func (n *LeafNode) NextNodeID() PageID {
	return n.nextNodeID
}

func (n *LeafNode) PrevNodeID() PageID {
	return n.prevNodeID
}

func (n *LeafNode) NextNode(noder Noder) *LeafNode {
	if n.nextNodeID < 0 {
		return nil
	}
	return noder.Read(n.nextNodeID).(*LeafNode)
}

func (n *LeafNode) PrevNode(noder Noder) *LeafNode {
	if n.prevNodeID < 0 {
		return nil
	}
	return noder.Read(n.prevNodeID).(*LeafNode)
}

func (n *LeafNode) SetID(id PageID) {
	n.id = id
}

func (n *LeafNode) ChildrenCount() int {
	return len(n.tuples)
}

func (n *LeafNode) Tuples() []*Tuple {
	return append([]*Tuple(nil), n.tuples...)
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

func (n *LeafNode) Update(newTuples []*Tuple, prevNodeID PageID, nextNodeID PageID) {
	n.tuples = newTuples
	n.prevNodeID = prevNodeID
	n.nextNodeID = nextNodeID
	n.page.MarkAsDirty()
	n.syncBytes()
}

func (n *LeafNode) syncBytes() {
	numTuples := uint32(len(n.tuples))
	copy(n.page.body[LEAF_NODE_NUM_TUPLES_OFFSET:LEAF_NODE_NUM_TUPLES_OFFSET+LEAF_NODE_NUM_TUPLE_SIZE], convertUint32ToBytes(numTuples))
	copy(n.page.body[LEAF_NODE_PREV_NODE_ID_OFFSET:LEAF_NODE_PREV_NODE_ID_OFFSET+LEAF_NODE_PREV_NODE_ID_SIZE], convertPageIDToBytes(n.prevNodeID))
	copy(n.page.body[LEAF_NODE_NEXT_NODE_ID_OFFSET:LEAF_NODE_NEXT_NODE_ID_OFFSET+LEAF_NODE_NEXT_NODE_ID_SIZE], convertPageIDToBytes(n.nextNodeID))

	offset := LEAF_NODE_FIRST_CHILD_OFFSET
	end := LEAF_NODE_FIRST_CHILD_OFFSET
	for _, tuple := range n.tuples {
		offset = end
		end = end + ROW_SIZE
		copy(n.page.body[offset:end], tuple.value)
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
	rootNodeID          PageID
	capacityPerLeafNode int
}

type Noder interface {
	Read(nodeId PageID) Node
	NewLeafNode(tuples []*Tuple) *LeafNode
	NewInternalNode(keys []uint32, children []PageID) *InternalNode
}

func (t *BTree) RootNode(noder Noder) Node {
	return noder.Read(t.rootNodeID)
}

func (t *BTree) String(noder Noder) string {
	message := ""
	// nodes := []Node{}
	// node := t.RootNode(noder)
	// for node != nil {
	// 	message = message + node.String() + "\n"
	// 	for _, nodeId := range node.Children() {
	// 		nodes = append(nodes, t.getNode(nodeId, noder))
	// 	}
	// 	if len(nodes) > 0 {
	// 		node, nodes = nodes[0], nodes[1:]
	// 	} else {
	// 		node = nil
	// 	}
	// }
	return message
}

func (t *BTree) getNode(nodeId PageID, noder Noder) Node {
	return noder.Read(nodeId)
}

func (t *BTree) newRoot(middleKey uint32, children []PageID, noder Noder) {
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
		leafNode.Update(newTuples, leafNode.PrevNodeID(), leafNode.NextNodeID())
	} else {
		midIdx := len(newTuples) / 2
		leafNode2 := noder.NewLeafNode(newTuples[midIdx:])
		leafNode2.Update(newTuples[midIdx:], leafNode.ID(), leafNode.NextNodeID())
		nextNode := leafNode.NextNode(noder)
		if nextNode != nil {
			nextNode.Update(nextNode.Tuples(), leafNode2.ID(), nextNode.NextNodeID())
		}
		leafNode.Update(newTuples[0:midIdx], leafNode.PrevNodeID(), leafNode2.ID())
		nodeID := leafNode2.ID()
		middleKey := newTuples[midIdx].key

		if len(nodes) == 0 {
			t.newRoot(middleKey, []PageID{leafNode.ID(), nodeID}, noder)

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
						t.newRoot(middleKey, []PageID{parentNode.id, nodeID}, noder)
					}
				}
			}
		}
	}

}

func (t *BTree) addChildrenToInternalNode(key uint32, nodeID PageID, internalNode *InternalNode, noder Noder) AddKeyResult {
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
	newNodeId PageID
}

// Find leaf L where entry belongs.
// Remove the entry.
// If L is at least half-full, done!
// If L has only M/2-1 entries,
//  * Try to re-distribute, borrowing from sibling(adjacent node with same parent as L)
//  * If re-distribute fails, merge L and sibling.
// If merge occured, must delete entry (pointing to L or sibling) from parent of L
// func (t *BTree) Delete0(key uint32, noder Noder) {
// 	rootNode := t.RootNode(noder)
// 	nodes := t.lookup([]Node{rootNode}, key, noder)
// 	leafNode := nodes[len(nodes)-1].(*LeafNode)
// 	firstKey := leafNode.Keys()[0]
// 	newTuples := removeTuple(key, leafNode)
// 	capacityThreshold := t.capacityPerLeafNode / 2
// 	var newFirstKey *uint32
// 	if len(leafNode.Keys()) > 0 {
// 		newFirstKey = &leafNode.Keys()[0]
// 	}

// 	var parentNode *InternalNode
// 	if len(nodes) > 1 {
// 		parentNode = nodes[len(nodes)-2].(*InternalNode)
// 	}

// 	if len(newTuples) >= capacityThreshold {
// 		if parentNode != nil && newFirstKey != nil && firstKey != *newFirstKey {
// 			replaceParentNodeKey(parentNode, firstKey, *newFirstKey)
// 		}
// 	} else if parentNode != nil {
// 		prevNode, nextNode := getSiblingNodes(leafNode, parentNode, noder)

// 		if canBorrowTuple(prevNode, capacityThreshold) {
// 			prevNodeTuples := prevNode.Tuples()
// 			borrowedTuple := prevNodeTuples[len(prevNodeTuples)-1]
// 			prevNode.Update(prevNodeTuples[:len(prevNodeTuples)-1], prevNode.PrevNodeID(), prevNode.NextNodeID())

// 			newTuples = append([]*Tuple{borrowedTuple}, newTuples...)
// 			leafNode.Update(newTuples, leafNode.PrevNodeID(), leafNode.NextNodeID())

// 			newKey := borrowedTuple.key
// 			replaceParentNodeKey(parentNode, firstKey, newKey)
// 		} else if canBorrowTuple(nextNode, capacityThreshold) {
// 			nextNodeTuples := nextNode.Tuples()
// 			borrowedTuple := nextNodeTuples[0]
// 			nextNode.Update(nextNodeTuples[1:], nextNode.PrevNodeID(), nextNode.NextNodeID())

// 			newTuples = append(newTuples, borrowedTuple)
// 			leafNode.Update(newTuples, leafNode.PrevNodeID(), leafNode.NextNodeID())

// 			newKey := nextNode.Keys()[0]
// 			replaceParentNodeKey(parentNode, borrowedTuple.key, newKey)
// 		} else {
// 			// TODO: merge
// 			// if left sibling exists, merge it
// 			// 	delete parent's first childten that is > lefy sibling's first key
// 			// otherwise merge with right sibling
// 			//	delete right sibling's first key from parent's keys
// 			// merge()
// 			// prevNode, nextNode := getSiblingNodes(leafNode, parentNode, noder)
// 			// if prevNode != nil {
// 			// 	t.merge(prevNode, leafNode)
// 			// } else {
// 			// 	t.merge(leafNode, nextNode)
// 			// }
// 		}

// 	}
// }

// Find leaf L where entry belongs.
// Remove the entry.
// If L is at least half-full, done!
// If L has only M/2-1 entries,
//  * Try to re-distribute, borrowing from sibling(adjacent node with same parent as L)
//  * If re-distribute fails, merge L and sibling.
// If merge occured, must delete entry (pointing to L or sibling) from parent of L
func (t *BTree) Delete(key uint32, noder Noder) {
	rootNode := t.RootNode(noder)
	nodes := t.lookup([]Node{rootNode}, key, noder)
	leafNode := nodes[len(nodes)-1].(*LeafNode)
	t.deleteEntry(leafNode, key, noder)
}

func deleteLeafNode(node LeafNode, key uint32, noder Noder) {
	// find entry by key
	// delete entry
	// if len(node.entries) > floor(node.capacityPerLeafNode / 2)
	//   call replaceParentNode(parentNode, oldKey, newKey)
	// else
	//   try re-distribute
	// 	 if re-distribute failed, merge L and sibling
	//   call deleteInternalNode(parentNode, key, noder)
}

func redistributeLeafNodes(leafNode *LeafNode, parentNode *InternalNode, newTuples []*Tuple,
	capacityThreshold int, firstKey uint32, noder Noder) bool {
	prevNode, nextNode := getSiblingLeafNodes(leafNode, parentNode, noder)
	if prevNode != nil && canBorrowChildren(prevNode, capacityThreshold) {
		prevNodeTuples := prevNode.Tuples()
		borrowedTuple := prevNodeTuples[len(prevNodeTuples)-1]
		prevNode.Update(prevNodeTuples[:len(prevNodeTuples)-1], prevNode.PrevNodeID(), prevNode.NextNodeID())

		newTuples = append([]*Tuple{borrowedTuple}, newTuples...)
		leafNode.Update(newTuples, leafNode.PrevNodeID(), leafNode.NextNodeID())

		newKey := borrowedTuple.key
		replaceParentNodeKey(parentNode, firstKey, newKey)
		return true
	} else if nextNode != nil && canBorrowChildren(nextNode, capacityThreshold) {
		nextNodeTuples := nextNode.Tuples()
		borrowedTuple := nextNodeTuples[0]
		nextNode.Update(nextNodeTuples[1:], nextNode.PrevNodeID(), nextNode.NextNodeID())

		newTuples = append(newTuples, borrowedTuple)
		leafNode.Update(newTuples, leafNode.PrevNodeID(), leafNode.NextNodeID())

		newKey := nextNode.Keys()[0]
		replaceParentNodeKey(parentNode, borrowedTuple.key, newKey)
		return true
	}
	return false
}

func redistributeInternalNodes(internalNode *InternalNode, parentNode *InternalNode,
	capacityThreshold int, firstKey uint32, noder Noder) bool {
	prevNode, nextNode := getSiblingInternalNodes(internalNode, parentNode, noder)
	if prevNode != nil && canBorrowChildren(prevNode, capacityThreshold) {
		prevNodePageIDs := prevNode.PageIDs()
		borrowedPageID, prevNodePageIDs := prevNodePageIDs[len(prevNodePageIDs)-1], prevNodePageIDs[:len(prevNodePageIDs)-1]
		prevNodeKeys := prevNode.Keys()
		newKey, prevNodeKeys := prevNodeKeys[len(prevNodeKeys)-1], prevNodeKeys[:len(prevNodeKeys)-1]
		prevNode.Update(prevNodeKeys, prevNodePageIDs)

		newPageIDs := append([]PageID{borrowedPageID}, prevNode.PageIDs()...)
		newKeys := append([]uint32{newKey}, internalNode.Keys()...)
		internalNode.Update(newKeys, newPageIDs)

		replaceParentNodeKey(parentNode, firstKey, newKey)
		return true
	} else if nextNode != nil && canBorrowChildren(nextNode, capacityThreshold) {
		prevNodePageIDs := prevNode.PageIDs()
		borrowedPageID, prevNodePageIDs := prevNodePageIDs[len(prevNodePageIDs)-1], prevNodePageIDs[:len(prevNodePageIDs)-1]
		prevNodeKeys := prevNode.Keys()
		newKey, prevNodeKeys := prevNodeKeys[len(prevNodeKeys)-1], prevNodeKeys[:len(prevNodeKeys)-1]
		prevNode.Update(prevNodeKeys, prevNodePageIDs)

		newPageIDs := append([]PageID{borrowedPageID}, prevNode.PageIDs()...)
		newKeys := append([]uint32{newKey}, internalNode.Keys()...)
		internalNode.Update(newKeys, newPageIDs)

		replaceParentNodeKey(parentNode, firstKey, newKey)
		return true
	}
	return false
}

func (t *BTree) deleteEntry(node Node, key uint32, noder Noder) {
	parentNode, _ := t.findParent(node, noder)
	capacityThreshold := t.capacityPerLeafNode / 2

	if node.NodeType() == "LeafNode" {
		leafNode := node.(*LeafNode)
		firstKey := leafNode.Keys()[0]
		newTuples := removeTuple(key, leafNode)
		var newFirstKey *uint32
		if len(leafNode.Keys()) > 0 {
			newFirstKey = &leafNode.Keys()[0]
		}
		if len(newTuples) >= capacityThreshold {
			if parentNode != nil && newFirstKey != nil && firstKey != *newFirstKey {
				replaceParentNodeKey(parentNode, firstKey, *newFirstKey)
			}
		} else if parentNode != nil {
			prevNode, nextNode := getSiblingLeafNodes(leafNode, parentNode, noder)

			result := redistributeLeafNodes(leafNode, parentNode, newTuples, capacityThreshold, firstKey, noder)
			if result == false {
				if prevNode != nil {
					t.mergeLeafNodes(prevNode, leafNode)
					deletedKey := firstKey
					t.deleteEntry(parentNode, deletedKey, noder)
				} else {
					t.mergeLeafNodes(leafNode, nextNode)
					deletedKey := nextNode.Keys()[0]
					t.deleteEntry(parentNode, deletedKey, noder)
				}
			}
		}
	} else {
		// TODO: handle internalNode redistribute and merge issue
		// capacity is account on children size
		internalNode := node.(*InternalNode)
		firstKey := internalNode.Keys()[0]
		removeKeyFromNode(key, internalNode)
		var newFirstKey *uint32
		if len(internalNode.Keys()) > 0 {
			newFirstKey = &internalNode.Keys()[0]
		}

		if internalNode.ChildrenCount() >= capacityThreshold {
			if parentNode != nil && newFirstKey != nil && firstKey != *newFirstKey {
				replaceParentNodeKey(parentNode, firstKey, *newFirstKey)
			}
		} else if parentNode != nil {
			result := redistributeInternalNodes(internalNode, parentNode, capacityThreshold, firstKey, noder)
			if result == false {
				// prevNode, nextNode := getSiblingInternalNodes(internalNode, parentNode, noder)
				// if prevNode != nil {
				// 	t.merge(prevNode, leafNode)
				// 	deletedKey := firstKey
				// 	t.deleteEntry(parentNode, deletedKey, noder)
				// } else {
				// 	t.merge(leafNode, nextNode)
				// 	deletedKey := nextNode.Keys()[0]
				// 	t.deleteEntry(parentNode, deletedKey, noder)
				// }
			}
			// parentNode.Children()
		}
	}
}

func removeKeyFromNode(key uint32, node *InternalNode) {
	keys := []uint32{}
	pageIDs := []PageID{node.children[0]}
	for idx, k := range node.Keys() {
		if k != key {
			keys = append(keys, k)
			pageIDs = append(pageIDs, node.PageIDs()[idx+1])
		}
	}
	node.Update(keys, pageIDs)
}

func (t *(BTree)) findParent(node Node, noder Noder) (*InternalNode, error) {
	key := node.Keys()[0]
	rootNode := t.RootNode(noder)
	nodes := t.lookup([]Node{rootNode}, key, noder)
	for _, n := range nodes {
		if n.NodeType() == "LeafNode" {
			return nil, errors.New("can't parent node")
		}
		for _, pageId := range n.(*InternalNode).PageIDs() {
			if node.ID() == pageId {
				return n.(*InternalNode), nil
			}
		}
	}
	return nil, nil
}

func (t *BTree) mergeLeafNodes(node1 *LeafNode, node2 *LeafNode) {
	newTuples := append(node1.Tuples(), node2.Tuples()...)
	node1.Update(newTuples, node1.prevNodeID, node2.nextNodeID)
}

func replaceParentNodeKey(node *InternalNode, oldKey uint32, newKey uint32) {
	keys := node.Keys()
	for idx, k := range keys {
		if k == oldKey {
			keys[idx] = newKey
		}
	}
	node.Update(keys, node.PageIDs())
}

func removeTuple(key uint32, node *LeafNode) []*Tuple {
	newTuples := []*Tuple{}
	for _, t := range node.Tuples() {
		if t.key != key {
			newTuples = append(newTuples, t)
		}
	}
	node.Update(newTuples, node.PrevNodeID(), node.NextNodeID())
	return newTuples
}

func getSiblingInternalNodes(node *InternalNode, parentNode *InternalNode, noder Noder) (*InternalNode, *InternalNode) {
	nodePageID := node.ID()
	var prevNode Node
	var nextNode Node
	pageIds := parentNode.PageIDs()
	for idx, pageID := range pageIds {
		if pageID == nodePageID {
			prevIdx := idx - 1
			if prevIdx >= 0 {
				prevNode = noder.Read(pageIds[prevIdx])
			}

			nextIdx := idx + 1
			if nextIdx < len(pageIds) {
				nextNode = noder.Read(pageIds[nextIdx])
			}
		}
	}

	return prevNode.(*InternalNode), nextNode.(*InternalNode)
}

func getSiblingLeafNodes(leafNode *LeafNode, parentNode *InternalNode, noder Noder) (*LeafNode, *LeafNode) {
	prevNode := leafNode.PrevNode(noder)
	nextNode := leafNode.NextNode(noder)
	if prevNode != nil && !parentNode.Contains(prevNode.ID()) {
		prevNode = nil
	}
	if nextNode != nil && !parentNode.Contains(nextNode.ID()) {
		nextNode = nil
	}
	return prevNode, nextNode
}

func canBorrowChildren(node Node, capacityThreshold int) bool {
	return node.ChildrenCount() >= capacityThreshold+1
}

func compare(val1 uint32, val2 uint32, operator string) bool {
	switch operator {
	case "=":
		return val1 == val2
	case ">=":
		return val1 >= val2
	case "<=":
		return val1 <= val2
	case ">":
		return val1 > val2
	case "<":
		return val1 < val2
	default:
		return false
	}
}

// FindLeafNodeByCondition return tuple index in the leafNode, return -1 if not found
func (t *BTree) FindLeafNodeByCondition(key uint32, operator string, noder Noder) (*LeafNode, int) {
	rootNode := t.RootNode(noder)
	nodes := t.lookup([]Node{rootNode}, key, noder)
	leafNode := nodes[len(nodes)-1].(*LeafNode)
	idx := -1
	for i, k := range leafNode.Keys() {
		if compare(k, key, operator) {
			idx = i
			break
		}
	}

	if idx != -1 || operator == "=" {
		return leafNode, idx
	} else if operator == ">=" || operator == ">" {
		nextNode := leafNode.NextNode(noder)
		for idx == -1 && nextNode != nil {
			for i, k := range nextNode.Keys() {
				if compare(k, key, operator) {
					leafNode = nextNode
					idx = i
					break
				}
			}
			nextNode = nextNode.NextNode(noder)
		}
		return leafNode, idx
	} else {
		prevNode := leafNode.PrevNode(noder)
		for idx == -1 && prevNode != nil {
			for i, k := range prevNode.Keys() {
				if compare(k, key, operator) {
					leafNode = prevNode
					idx = i
					break
				}
			}
			prevNode = prevNode.PrevNode(noder)
		}
		return leafNode, idx
	}
}

func (t *BTree) FindLeafNode(key uint32, noder Noder) *LeafNode {
	rootNode := t.RootNode(noder)
	nodes := t.lookup([]Node{rootNode}, key, noder)
	return nodes[len(nodes)-1].(*LeafNode)
}

func (t *BTree) Find(key uint32, noder Noder) *Tuple {
	leafNode := t.FindLeafNode(key, noder)
	if leafNode == nil {
		return nil
	}

	for _, tuple := range leafNode.Tuples() {
		if tuple.key == key {
			return tuple
		}
	}
	return nil
}

func (t *BTree) First(noder Noder) *Tuple {
	node := t.RootNode(noder)
	for node.NodeType() == "InternalNode" {
		leftChild := node.(*InternalNode).PageIDs()[0]
		node = t.getNode(leftChild, noder)
	}
	leafNode := node.(*LeafNode)
	return leafNode.Tuples()[0]
}

// Return left most leaf node
func (t *BTree) FirstLeafNode(noder Noder) *LeafNode {
	node := t.RootNode(noder)
	for node.NodeType() == "InternalNode" {
		leftChild := node.(*InternalNode).PageIDs()[0]
		node = t.getNode(leftChild, noder)
	}
	leafNode := node.(*LeafNode)
	return leafNode
}

// Return node's right sibling
func (t *BTree) NextLeafNode(node *LeafNode, noder Noder) *LeafNode {
	return node.NextNode(noder)
}

// Return node's left sibling
func (t *BTree) PrevLeafNode(node *LeafNode, noder Noder) *LeafNode {
	return node.PrevNode(noder)
}

// return node and it's ancestor nodes
func (t *BTree) lookup(nodes []Node, key uint32, noder Noder) []Node {
	node := nodes[len(nodes)-1]

	if node.NodeType() == "LeafNode" {
		return nodes
	}

	idx := 0
	for _, k := range node.Keys() {
		if key < k {
			break
		}
		idx++
	}
	nodeId := node.(*InternalNode).PageIDs()[idx]
	newNodes := append(nodes, t.getNode(nodeId, noder))
	return t.lookup(newNodes, key, noder)
}
