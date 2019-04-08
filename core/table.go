package core

import (
	"encoding/binary"
	"fmt"
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

type Page struct {
	rows [ROW_PER_PAGE]*Row
}

func (p *Page) Rows() [ROW_PER_PAGE]*Row {
	return p.rows
}

func (p *Page) Bytes() []byte {
	bs := []byte{}
	for _, row := range p.rows {
		if row.id == 0 {
			break
		}
		bs = append(bs, row.Bytes()...)
	}
	return bs
}

const TABLE_MAX_PAGES = 100
const TABLE_MAX_ROWS = TABLE_MAX_PAGES * ROW_PER_PAGE

type Table struct {
	numRows int
	pages   [TABLE_MAX_PAGES]*Page
	pager   *Pager
	btree   *BTree
}

func OpenBtree(fileName string) (*BTree, error) {
	pager, err := OpenPager2(fileName)
	if err != nil {
		return nil, err
	}
	fileNoder := &FileNoder{pager: pager}
	header, err := fileNoder.ReadTableHeader()
	if err != nil {
		return nil, err
	}

	rootNode := fileNoder.Read(header.rootPageNum)
	btree := &BTree{
		rootNode:        rootNode,
		capacityPerNode: ROW_PER_PAGE,
		noder:           fileNoder,
	}
	return btree, nil
}

func OpenTable(fileName string) (*Table, error) {
	pager, _ := OpenPager(fileName)
	fileSize, err := pager.FileSize()
	if err != nil {
		return nil, err
	}
	numRows := (fileSize/PAGE_SIZE)*14 + (fileSize%PAGE_SIZE)/ROW_SIZE
	rootNode := &LeafNode{
		id:     0,
		tuples: []*Tuple{},
	}
	btree := &BTree{
		rootNode:        rootNode,
		capacityPerNode: ROW_PER_PAGE,
	}
	return &Table{
		numRows: int(numRows),
		pager:   pager,
		btree:   btree,
	}, nil
}

func (t *Table) CloseTable() error {
	for pageNum, page := range t.pager.pages {
		if page != nil {
			err := t.pager.FlushPage(pageNum)
			if err != nil {
				return err
			}
		}
	}
	err := t.pager.file.Close()
	return err
}

func (t *Table) rowSlot(rowNum int) (*Row, error) {
	pageNum := rowNum / ROW_PER_PAGE
	page, err := t.pager.ReadPage(pageNum)
	if err != nil {
		return nil, err
	}
	return page.rows[rowNum%ROW_PER_PAGE], nil
}

func (t *Table) InsertRow(newRow *Row) error {
	c := newCursorFromEnd(t)
	row, err := c.value()
	if err != nil {
		return err
	}
	row.update(newRow)
	t.numRows = t.numRows + 1
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

func (t *Table) Pages() [TABLE_MAX_PAGES]*Page {
	return t.pages
}

// Cursor represents a location in the table.
type Cursor struct {
	table      *Table
	endOfTable bool
	pageNum    int
	cellNum    int
	// Deprecated
	rowNum int
}

// * Create a cursor at the beginning of the table
func newCursorFromStart(table *Table) *Cursor {
	return &Cursor{
		table:      table,
		endOfTable: table.numRows == 0,
		// pageNum:    table.btree.rootNode.id,
		rowNum: 0,
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
	rowNum := c.rowNum
	pageNum := rowNum / ROW_PER_PAGE
	page, err := c.table.pager.ReadPage(pageNum)
	if err != nil {
		return nil, err
	}
	return page.rows[rowNum%ROW_PER_PAGE], nil
}

// Advance the cursor to the next row
func (c *Cursor) advance() {
	c.rowNum = c.rowNum + 1
	if c.rowNum >= c.table.numRows {
		c.endOfTable = true
	}
}
