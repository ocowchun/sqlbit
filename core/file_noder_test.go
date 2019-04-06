package core

import (
	"bufio"
	"encoding/binary"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadTableHeader(t *testing.T) {
	removeTestFile()
	fileName := getTestFileName()
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	w := bufio.NewWriter(f)
	bs := make([]byte, 2)
	binary.LittleEndian.PutUint16(bs, uint16(PAGE_TYPE_TABLE_HEADER))
	rootPageNum := uint32(2)
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, rootPageNum)
	bs = append(bs, b...)
	_, err = w.Write(bs)
	w.Flush()
	f.Close()
	pager, _ := OpenPager2(fileName)
	fileNoder := &FileNoder{pager: pager}

	header, err := fileNoder.ReadTableHeader()

	assert.Nil(t, err)
	assert.Equal(t, rootPageNum, header.rootPageNum)
	removeTestFile()
}

func TestReadInternalNode(t *testing.T) {
	removeTestFile()
	fileName := getTestFileName()
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	w := bufio.NewWriter(f)
	bs := make([]byte, 2)
	binary.LittleEndian.PutUint16(bs, uint16(PAGE_TYPE_INTERNAL_NODE))
	numKeys := uint32(2)
	child1 := uint32(7)
	key1 := uint32(5)
	child2 := uint32(19)
	key2 := uint32(12)
	child3 := uint32(25)

	for _, num := range []uint32{numKeys, child1, key1, child2, key2, child3} {
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, num)
		bs = append(bs, b...)
	}

	bs = append(make([]byte, 4096), bs...)
	_, err = w.Write(bs)
	w.Flush()
	f.Close()
	pager, _ := OpenPager2(fileName)
	fileNoder := &FileNoder{pager: pager}

	node := fileNoder.Read(1)

	assert.Nil(t, err)
	assert.Equal(t, "InternalNode", node.NodeType())
	assert.Equal(t, uint32(1), node.ID())
	assert.Equal(t, []uint32{key1, key2}, node.Keys())
	assert.Equal(t, []uint32{child1, child2, child3}, node.Children())
	removeTestFile()
}

func TestReadNodeFromNodeMap(t *testing.T) {
	nodeMap := make(map[uint32]Node)
	nodeId := uint32(2)
	nodeMap[nodeId] = &InternalNode{id: nodeId}
	fileNoder := &FileNoder{nodeMap: nodeMap}

	node := fileNoder.Read(nodeId)

	assert.Equal(t, "InternalNode", node.NodeType())
	assert.Equal(t, nodeId, node.ID())
}

func createTuple(key uint32) *Tuple {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, key)

	return &Tuple{key: key, value: append(bs, make([]byte, ROW_SIZE-4)...)}
}
func TestReadLeafNode(t *testing.T) {
	removeTestFile()
	fileName := getTestFileName()
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	w := bufio.NewWriter(f)
	bs := make([]byte, 2)
	binary.LittleEndian.PutUint16(bs, uint16(PAGE_TYPE_LEAF_NODE))
	tuples := []*Tuple{createTuple(17), createTuple(42)}
	numTuples := uint32(len(tuples))
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, numTuples)
	bs = append(bs, b...)

	for _, tuple := range tuples {
		bs = append(bs, tuple.value...)
	}

	bs = append(make([]byte, 4096), bs...)
	_, err = w.Write(bs)
	w.Flush()
	f.Close()
	pager, _ := OpenPager2(fileName)
	fileNoder := &FileNoder{pager: pager}

	node := fileNoder.Read(1)

	assert.Nil(t, err)
	assert.Equal(t, "LeafNode", node.NodeType())
	assert.Equal(t, uint32(1), node.ID())
	assert.Equal(t, []uint32{uint32(17), uint32(42)}, node.Keys())
	removeTestFile()
}

func TestAddNode(t *testing.T) {
	removeTestFile()
	fileName := getTestFileName()
	pager, _ := OpenPager2(fileName)

	nodeMap := make(map[uint32]Node)
	fileNoder := &FileNoder{pager: pager, nodeMap: nodeMap}

	node := &InternalNode{}
	nodeId := fileNoder.Add(node)

	assert.Equal(t, uint32(1), nodeId)
	removeTestFile()
}
