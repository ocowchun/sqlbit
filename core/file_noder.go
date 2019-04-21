package core

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

type FileNoder struct {
	pager   *Pager2
	nodeMap map[uint32]Node
}

func NewFileNoder(pager *Pager2) *FileNoder {
	nodeMap := make(map[uint32]Node)
	return &FileNoder{
		pager:   pager,
		nodeMap: nodeMap,
	}
}

const PAGE_TYPE_SIZE = 2
const PAGE_TYPE_TABLE_HEADER = 0
const PAGE_TYPE_INTERNAL_NODE = 1
const PAGE_TYPE_LEAF_NODE = 2

const TABLE_HEADER_ROOT_PAGE_NUM_SIZE = 4
const TABLE_HEADER_HEADER_SIZE = PAGE_TYPE_SIZE + TABLE_HEADER_ROOT_PAGE_NUM_SIZE

type TableHeader struct {
	rootPageNum uint32
}

func (n FileNoder) ReadTableHeader() (*TableHeader, error) {
	bytes, err := n.pager.ReadPage(0)

	if err != nil {
		return nil, err
	}
	bs := bytes[:PAGE_TYPE_SIZE]
	pageType := binary.LittleEndian.Uint16(bs)

	if pageType != PAGE_TYPE_TABLE_HEADER {
		return nil, errors.New("Incorrect page_type for Table Header")
	}
	from := PAGE_TYPE_SIZE
	bs = bytes[from : from+TABLE_HEADER_ROOT_PAGE_NUM_SIZE]
	rootPageNum := binary.LittleEndian.Uint32(bs)
	return &TableHeader{rootPageNum: rootPageNum}, nil
}

func serializeTableHeader(tree *BTree) []byte {
	bs := make([]byte, 2)
	binary.LittleEndian.PutUint16(bs, uint16(PAGE_TYPE_TABLE_HEADER))

	rootPageNum := uint32(tree.rootNode.ID())
	bs = append(bs, convertUint32ToBytes(rootPageNum)...)
	return append(bs, make([]byte, PAGE_SIZE-len(bs))...)
}

func (n *FileNoder) Read(nodeId uint32) Node {
	if n.nodeMap[nodeId] != nil {
		return n.nodeMap[nodeId]
	}

	bytes, err := n.pager.ReadPage(nodeId)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
		return nil
	}

	bs := bytes[:PAGE_TYPE_SIZE]

	pageType := binary.LittleEndian.Uint16(bs)
	var node Node
	if pageType == PAGE_TYPE_INTERNAL_NODE {
		node = deserializeInternalNode(nodeId, bytes)
	} else if pageType == PAGE_TYPE_LEAF_NODE {
		node = deserializeLeafNode(nodeId, bytes)
	} else {
		fmt.Println("You can't convert unknwon page to node")
		os.Exit(1)
		return nil
	}
	if node != nil {
		n.nodeMap[nodeId] = node
	}
	return node
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

func serializeInternalNode(node *InternalNode) []byte {
	bs := make([]byte, 2)
	binary.LittleEndian.PutUint16(bs, uint16(PAGE_TYPE_INTERNAL_NODE))

	numKeys := uint32(len(node.keys))
	bs = append(bs, convertUint32ToBytes(numKeys)...)

	bs = append(bs, convertUint32ToBytes(node.children[0])...)
	for idx, key := range node.keys {
		bs = append(bs, convertUint32ToBytes(key)...)
		bs = append(bs, convertUint32ToBytes(node.children[idx+1])...)
	}
	return bs
}

func convertUint32ToBytes(num uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, num)
	return b
}

const LEAF_NODE_NUM_TUPLE_SIZE = 4
const LEAF_NODE_HEADER_SIZE = PAGE_TYPE_SIZE + LEAF_NODE_NUM_TUPLE_SIZE
const LEAF_NODE_CHILD_SIZE = ROW_SIZE
const LEAF_NODE_KEY_PER_PAGE = (PAGE_SIZE - LEAF_NODE_HEADER_SIZE) / LEAF_NODE_CHILD_SIZE

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

func serializeLeafNode(node *LeafNode) []byte {
	bs := make([]byte, 2)
	binary.LittleEndian.PutUint16(bs, uint16(PAGE_TYPE_LEAF_NODE))

	numTuples := uint32(len(node.tuples))
	bs = append(bs, convertUint32ToBytes(numTuples)...)

	for _, tuple := range node.tuples {
		bs = append(bs, tuple.value...)
	}
	return bs
}

func (n *FileNoder) add(node Node) uint32 {
	pageNum := n.pager.IncrementPageNum()
	nodeId := uint32(pageNum)
	node.SetID(nodeId)
	n.nodeMap[nodeId] = node
	return nodeId
}

func (n *FileNoder) NewLeafNode(tuples []*Tuple) ILeafNode {
	node := &LeafNode{tuples: tuples}
	id := n.add(node)
	node.SetID(id)
	return node
}

func (n *FileNoder) NewInternalNode(keys []uint32, children []uint32) *InternalNode {
	node := &InternalNode{
		keys:     keys,
		children: children,
	}

	id := n.add(node)
	node.SetID(id)
	return node
}

func (n *FileNoder) Save(tree *BTree) error {
	b := serializeTableHeader(tree)
	err := n.pager.FlushPage(0, b)
	if err != nil {
		return err
	}

	for pageNum, node := range n.nodeMap {
		var bs []byte
		if node.NodeType() == "InternalNode" {
			bs = serializeInternalNode(node.(*InternalNode))
		} else if node.NodeType() == "LeafNode" {
			bs = serializeLeafNode(node.(*LeafNode))
		} else {
			return errors.New("can't save invalid node to file")
		}
		err := n.pager.FlushPage(int(pageNum), bs)
		if err != nil {
			return err
		}
	}
	return nil
}
