package core

import (
	"encoding/binary"
	"fmt"
	"os"
)

type TransactionNoder struct {
	transaction *Transaction
}

func (n *TransactionNoder) Read(nodeID uint32) Node {
	page, err := n.transaction.ReadPage(nodeID)
	// TODO: handle error with better way later
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
		return nil
	}
	bs := page[:PAGE_TYPE_SIZE]
	pageType := binary.LittleEndian.Uint16(bs)

	var node Node
	if pageType == PAGE_TYPE_INTERNAL_NODE {
		node = deserializeInternalNodeFromPage(nodeID, page)
	} else if pageType == PAGE_TYPE_LEAF_NODE {
		node = deserializeLeafNodeFromPage(nodeID, page)
	} else {
		fmt.Println("You can't convert unknwon page to node")
		os.Exit(1)
		return nil
	}

	return node
}

func deserializeInternalNodeFromPage(nodeId uint32, page *Page) *InternalNode {
	keys := []uint32{}
	children := []uint32{}
	from := PAGE_TYPE_SIZE
	bs := page[from : from+INTERNAL_NODE_NUM_KEYS_SIZE]
	numKeys := binary.LittleEndian.Uint32(bs)
	from = from + INTERNAL_NODE_NUM_KEYS_SIZE

	bs = page[from : from+INTERNAL_NODE_CHILD_SIZE]
	children = append(children, binary.LittleEndian.Uint32(bs))
	from = from + INTERNAL_NODE_CHILD_SIZE

	for i := 0; i < int(numKeys); i++ {
		bs := page[from : from+INTERNAL_NODE_KEY_SIZE]

		keys = append(keys, binary.LittleEndian.Uint32(bs))
		from = from + INTERNAL_NODE_KEY_SIZE

		bs = page[from : from+INTERNAL_NODE_CHILD_SIZE]
		children = append(children, binary.LittleEndian.Uint32(bs))
		from = from + INTERNAL_NODE_CHILD_SIZE
	}

	return &InternalNode{
		id:       nodeId,
		keys:     keys,
		children: children,
		bytes:    page,
	}
}

func deserializeLeafNodeFromPage(nodeId uint32, page *Page) *LeafNode {
	tuples := []*Tuple{}
	from := PAGE_TYPE_SIZE
	bs := page[from : from+LEAF_NODE_NUM_TUPLE_SIZE]
	numTuples := binary.LittleEndian.Uint32(bs)
	from = from + INTERNAL_NODE_NUM_KEYS_SIZE

	for i := 0; i < int(numTuples); i++ {
		bs := page[from : from+ROW_SIZE]
		key := binary.LittleEndian.Uint32(bs[:4])
		tuples = append(tuples, &Tuple{key: key, value: bs})
		from = from + ROW_SIZE
	}

	return &LeafNode{
		id:     nodeId,
		tuples: tuples,
		bytes:  page,
	}
}

func (n *TransactionNoder) NewLeafNode(tuples []*Tuple) *LeafNode {
	pageWithPageID, err := n.transaction.bufferPool.NewPage()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
		return nil
	}
	node := &LeafNode{
		id:     pageWithPageID.pageID,
		tuples: tuples,
		bytes:  pageWithPageID.page,
	}
	node.syncBytes()
	return node
}

func (n *TransactionNoder) NewInternalNode(keys []uint32, children []uint32) *InternalNode {
	pageWithPageID, err := n.transaction.bufferPool.NewPage()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
		return nil
	}

	node := &InternalNode{
		id:       pageWithPageID.pageID,
		keys:     keys,
		children: children,
		bytes:    pageWithPageID.page,
	}
	node.syncBytes()
	return node
}
