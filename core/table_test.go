package core

import (
	"bufio"
	"encoding/binary"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func prepareBtreeFile(fileName string, tuples []*Tuple) uint32 {
	f, _ := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	w := bufio.NewWriter(f)

	//Prepare Table Header
	bs := make([]byte, 2)
	binary.LittleEndian.PutUint16(bs, uint16(PAGE_TYPE_TABLE_HEADER))
	rootPageNum := uint32(1)
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, rootPageNum)
	bs = append(bs, b...)
	bs = append(bs, make([]byte, 4096-len(bs))...)

	//Prepare Leaf Node
	b = make([]byte, 2)
	binary.LittleEndian.PutUint16(b, uint16(PAGE_TYPE_LEAF_NODE))
	bs = append(bs, b...)
	numTuples := uint32(len(tuples))
	b = make([]byte, 4)
	binary.LittleEndian.PutUint32(b, numTuples)
	bs = append(bs, b...)
	for _, tuple := range tuples {
		bs = append(bs, tuple.value...)
	}

	w.Write(bs)
	w.Flush()
	f.Close()

	return rootPageNum
}
func TestOpenTable(t *testing.T) {
	removeTestFile()
	fileName := getTestFileName()
	tuples := []*Tuple{createTuple(17), createTuple(42)}
	rootPageNum := prepareBtreeFile(fileName, tuples)

	table, err := OpenTable(fileName)

	assert.Nil(t, err)
	assert.Equal(t, int(rootPageNum), int(table.btree.rootNodeID))
}

func TestTableSelect(t *testing.T) {
	removeTestFile()
	fileName := getTestFileName()
	tuples := []*Tuple{createTuple(17), createTuple(42)}
	prepareBtreeFile(fileName, tuples)
	table, err := OpenTable(fileName)

	rows, err := table.Select()

	assert.Nil(t, err)
	assert.Equal(t, len(tuples), len(rows))
	for idx, tuple := range tuples {
		assert.Equal(t, tuple.key, rows[idx].Id())
	}
}
