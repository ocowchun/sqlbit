package core

import (
	"encoding/binary"
	"fmt"
	"strings"
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

const PAGE_SIZE = 4096

// 4 + 32 + 255
const ROW_SIZE = 291
const ROW_PER_PAGE = PAGE_SIZE / ROW_SIZE

const TABLE_MAX_PAGES = 100
const TABLE_MAX_ROWS = TABLE_MAX_PAGES * ROW_PER_PAGE

type Table struct {
	numRows   int
	btree     *BTree
	pager2    *Pager2
	fileNoder *FileNoder
}

func OpenBtree(fileNoder *FileNoder) (*BTree, error) {
	header, err := fileNoder.ReadTableHeader()
	if err != nil {
		return nil, err
	}

	rootNode := fileNoder.Read(header.rootPageNum)
	btree := &BTree{
		rootNode:            rootNode,
		capacityPerLeafNode: ROW_PER_PAGE,
		// noder:               fileNoder,
	}
	return btree, nil
}

func OpenTable(fileName string) (*Table, error) {
	pager, err := OpenPager2(fileName)
	if err != nil {
		return nil, err
	}
	fileNoder := NewFileNoder(pager)

	btree, err := OpenBtree(fileNoder)
	if err != nil {
		return nil, err
	}
	return NewTable(btree, pager, fileNoder), nil
}

func NewTable(btree *BTree, pager *Pager2, fileNoder *FileNoder) *Table {
	return &Table{
		btree:     btree,
		pager2:    pager,
		fileNoder: fileNoder,
	}
}

func (t *Table) CloseTable() error {
	return t.fileNoder.Save(t.btree)
}

func (t *Table) InsertRow(newRow *Row) error {
	c := newCursorFromStart(t)
	c.write(newRow)
	return nil
}

func (t *Table) Select() ([]*Row, error) {
	rows := []*Row{}
	c := newCursorFromStart(t)
	for c.endOfTable != true {
		row, err := c.value()
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
		c.advance()
	}
	return rows, nil
}

func (t *Table) NumRows() int {
	return t.numRows
}

// Cursor represents a location in the table.
type Cursor struct {
	table      *Table
	endOfTable bool
	pageNum    int
	cellNum    int

	leafNode *LeafNode

	// Deprecated
	rowNum int
}

// * Create a cursor at the beginning of the table
func newCursorFromStart(table *Table) *Cursor {
	leafNode := table.btree.FirstLeafNode(table.fileNoder)
	return &Cursor{
		table:      table,
		endOfTable: len(leafNode.Keys()) == 0,
		leafNode:   leafNode,
		rowNum:     1,
	}
}

// Create a cursor at the end of the table
func newCursorFromEnd(table *Table) *Cursor {
	return &Cursor{
		table:      table,
		rowNum:     table.numRows,
		endOfTable: true,
	}
}

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
	c.table.btree.Insert(row.Id(), row.Bytes(), c.table.fileNoder)
}

// Advance the cursor to the next row
func (c *Cursor) advance() {
	c.cellNum = c.cellNum + 1
	if c.cellNum >= LEAF_NODE_KEY_PER_PAGE {
		newLeafNode := c.table.btree.NextLeafNode(c.leafNode, c.table.fileNoder)
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
