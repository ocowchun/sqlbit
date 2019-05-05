package core

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
)

const COLUMN_USERNAME_LENGTH = 32
const COLUMN_EMAIL_LENGTH = 255

type Row struct {
	id       uint32
	username string
	email    string
}

func NewRow(id uint32, username string, email string) *Row {
	return &Row{
		id:       id,
		username: username,
		email:    email,
	}
}

func (r *Row) Bytes() []byte {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, r.id)

	bs2 := make([]byte, COLUMN_USERNAME_LENGTH)
	copy(bs2[:], r.username)
	bs = append(bs, bs2...)

	bs3 := make([]byte, COLUMN_EMAIL_LENGTH)
	copy(bs3[:], r.email)
	bs = append(bs, bs3...)

	return bs
}

func (r *Row) Id() uint32 {
	return r.id
}

func (r *Row) Username() string {
	return r.username
}

func (r *Row) Email() string {
	return r.email
}

func (r *Row) String() string {
	return fmt.Sprintf("(%d, %s,%s)", r.Id(), r.Username(), r.Email())
}

func (r *Row) update(newRow *Row) {
	r.id = newRow.id
	r.username = newRow.username
	r.email = newRow.email
}

const PAGE_TYPE_SIZE = 2
const PAGE_TYPE_TABLE_HEADER = 0
const PAGE_TYPE_INTERNAL_NODE = 1
const PAGE_TYPE_LEAF_NODE = 2

const PAGE_SIZE = 4096

// 4 + 32 + 255
const ROW_SIZE = 291
const ROW_PER_PAGE = PAGE_SIZE / ROW_SIZE

const TABLE_MAX_PAGES = 100
const TABLE_MAX_ROWS = TABLE_MAX_PAGES * ROW_PER_PAGE

type Table struct {
	numRows           int
	btree             *BTree
	bufferPool        *BufferPool
	lastTransactionID int32
}

func OpenTable(fileName string) (*Table, error) {
	replacer := &DummyReplacer{
		frameIndices: []uint32{},
		pinnedIdxMap: make(map[uint32]bool),
	}
	pager, err := NewFilePager(fileName)
	if err != nil {
		return nil, err
	}

	bufferPool := NewBufferPool(replacer, pager, 5, 100)
	tableHeader, err := readTableHeader(bufferPool)
	if err != nil {
		return nil, err
	}

	btree := &BTree{
		rootNodeID:          tableHeader.rootPageNum,
		capacityPerLeafNode: ROW_PER_PAGE,
	}

	return &Table{
		btree:             btree,
		lastTransactionID: int32(0),
		bufferPool:        bufferPool,
	}, nil
}

const TABLE_HEADER_ROOT_PAGE_NUM_SIZE = 4
const TABLE_HEADER_HEADER_SIZE = PAGE_TYPE_SIZE + TABLE_HEADER_ROOT_PAGE_NUM_SIZE

type TableHeader struct {
	rootPageNum uint32
}

func readTableHeader(bufferPool *BufferPool) (*TableHeader, error) {
	page, err := bufferPool.FetchPage(uint32(0))

	if err != nil {
		return nil, err
	}
	bs := page[:PAGE_TYPE_SIZE]
	pageType := binary.LittleEndian.Uint16(bs)

	if pageType != PAGE_TYPE_TABLE_HEADER {
		return nil, errors.New("Incorrect page_type for Table Header")
	}
	from := PAGE_TYPE_SIZE
	bs = page[from : from+TABLE_HEADER_ROOT_PAGE_NUM_SIZE]
	rootPageNum := binary.LittleEndian.Uint32(bs)
	return &TableHeader{rootPageNum: rootPageNum}, nil
}

func NewTable(btree *BTree, bufferPool *BufferPool) *Table {
	return &Table{
		btree:             btree,
		bufferPool:        bufferPool,
		lastTransactionID: int32(0),
	}
}

func (t *Table) CloseTable() error {
	t.bufferPool.FlushAllPage()
	return nil
}

func (t *Table) newTransaction() *Transaction {
	id := atomic.AddInt32(&t.lastTransactionID, 1)
	return NewTransaction(id, t.bufferPool)
}

func (t *Table) InsertRow(newRow *Row) error {
	tx := t.newTransaction()
	c := newCursorFromStart(t, tx)
	c.write(newRow)
	tx.Commit()
	return nil
}

func (t *Table) Select() ([]*Row, error) {
	tx := t.newTransaction()

	rows := []*Row{}
	c := newCursorFromStart(t, tx)
	for c.endOfTable != true {
		row, err := c.value()
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
		c.advance()
	}
	tx.Commit()
	return rows, nil
}

func (t *Table) NumRows() int {
	return t.numRows
}

// Cursor represents a location in the table.
type Cursor struct {
	table      *Table
	noder      Noder
	endOfTable bool
	pageNum    int
	cellNum    int

	leafNode *LeafNode

	// Deprecated
	rowNum int
}

// * Create a cursor at the beginning of the table
func newCursorFromStart(table *Table, tx *Transaction) *Cursor {
	noder := &TransactionNoder{transaction: tx}
	leafNode := table.btree.FirstLeafNode(noder)
	return &Cursor{
		table:      table,
		noder:      noder,
		endOfTable: len(leafNode.Keys()) == 0,
		leafNode:   leafNode,
		rowNum:     1,
	}
}

// Create a cursor at the end of the table
// func newCursorFromEnd(table *Table) *Cursor {
// 	return &Cursor{
// 		table:      table,
// 		rowNum:     table.numRows,
// 		endOfTable: true,
// 	}
// }

// Access the row the cursor is pointing to
func (c *Cursor) value() (*Row, error) {
	tuple := c.leafNode.tuples[c.cellNum]
	bs := tuple.value
	id := binary.LittleEndian.Uint32(bs)
	replacer := strings.NewReplacer("\x00", "")
	usrename := replacer.Replace(string(bs[4:COLUMN_USERNAME_LENGTH]))
	email := replacer.Replace(string(bs[36:COLUMN_EMAIL_LENGTH]))

	row := NewRow(id, usrename, email)
	return row, nil
}

// Overwrite the row
func (c *Cursor) write(row *Row) {
	c.table.btree.Insert(row.Id(), row.Bytes(), c.noder)
}

// Advance the cursor to the next row
func (c *Cursor) advance() {
	c.cellNum = c.cellNum + 1
	if c.cellNum >= LEAF_NODE_KEY_PER_PAGE {
		newLeafNode := c.table.btree.NextLeafNode(c.leafNode, c.noder)
		if newLeafNode != nil {
			c.cellNum = 0
			c.leafNode = newLeafNode
		} else {
			c.endOfTable = true
		}
	} else if c.cellNum >= len(c.leafNode.Keys()) {
		c.endOfTable = true
	}
}
