package core

import (
	"encoding/binary"
	"fmt"
	"os"
)

type FileNoder struct {
	pager *Pager2
}

const PAGE_TYPE_SIZE = 2
const PAGE_TYPE_TABLE_HEADER = 0
const PAGE_TYPE_INTERNAL_NODE = 1
const PAGE_TYPE_LEAF_NODE = 2

func (n *FileNoder) Read(nodeId uint32) Node {
	bytes, err := n.pager.ReadPage(nodeId)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
		return nil
	}

	bs := bytes[:PAGE_TYPE_SIZE]

	pageType := binary.LittleEndian.Uint16(bs)
	if pageType == PAGE_TYPE_INTERNAL_NODE {
		return deserializeInternalNode(nodeId, bytes)
	} else if pageType == PAGE_TYPE_LEAF_NODE {
		return deserializeLeafNode(nodeId, bytes)
	} else {
		fmt.Println("You can't convert unknwon page to node")
		os.Exit(1)
		return nil
	}
}

const INTERNAL_NODE_NUM_KEYS_SIZE = 4
const INTERNAL_NODE_HEADER_SIZE = PAGE_TYPE_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE
const INTERNAL_NODE_CHILD_SIZE = 4
const INTERNAL_NODE_KEY_SIZE = 4
const INTERNAL_NODE_KEY_PER_PAGE = (PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE - INTERNAL_NODE_CHILD_SIZE) / (INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE)

func deserializeInternalNode(nodeId uint32, bytes []byte) *InternalNode {
	keys := []uint32{}
	children := []uint32{}
	from := PAGE_TYPE_SIZE
	bs := bytes[from : from+INTERNAL_NODE_NUM_KEYS_SIZE]
	numKeys := binary.LittleEndian.Uint32(bs)
	from = from + INTERNAL_NODE_NUM_KEYS_SIZE

	bs = bytes[from : from+INTERNAL_NODE_CHILD_SIZE]
	children = append(children, binary.LittleEndian.Uint32(bs))
	from = from + INTERNAL_NODE_CHILD_SIZE

	for i := 0; i < int(numKeys); i++ {
		bs := bytes[from : from+INTERNAL_NODE_KEY_SIZE]

		keys = append(keys, binary.LittleEndian.Uint32(bs))
		from = from + INTERNAL_NODE_KEY_SIZE

		bs = bytes[from : from+INTERNAL_NODE_CHILD_SIZE]
		children = append(children, binary.LittleEndian.Uint32(bs))
		from = from + INTERNAL_NODE_CHILD_SIZE
	}

	return &InternalNode{
		id:       nodeId,
		keys:     keys,
		children: children,
	}
}

const LEAF_NODE_NUM_TUPLE_SIZE = 4
const LEAF_NODE_HEADER_SIZE = PAGE_TYPE_SIZE + LEAF_NODE_NUM_TUPLE_SIZE
const LEAF_NODE_CHILD_SIZE = ROW_SIZE

func deserializeLeafNode(nodeId uint32, bytes []byte) *LeafNode {
	tuples := []*Tuple{}
	from := PAGE_TYPE_SIZE
	bs := bytes[from : from+LEAF_NODE_NUM_TUPLE_SIZE]
	numTuples := binary.LittleEndian.Uint32(bs)
	from = from + INTERNAL_NODE_NUM_KEYS_SIZE

	for i := 0; i < int(numTuples); i++ {
		bs := bytes[from : from+ROW_SIZE]
		key := binary.LittleEndian.Uint32(bs[:4])
		tuples = append(tuples, &Tuple{key: key, value: bs})
		from = from + ROW_SIZE
	}

	return &LeafNode{
		id:     nodeId,
		tuples: tuples,
	}
}
