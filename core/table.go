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

func (p *Page) InsertRow(idx int, row *Row) {
	p.rows[idx] = row
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
}

func OpenTable(fileName string) (*Table, error) {
	pager, _ := OpenPager(fileName)
	fileSize, err := pager.FileSize()
	if err != nil {
		return nil, err
	}
	numRows := (fileSize/PAGE_SIZE)*14 + (fileSize%PAGE_SIZE)/ROW_SIZE
	return &Table{
		numRows: int(numRows),
		pager:   pager,
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

func (t *Table) InsertRow(row *Row) error {
	pageNum := t.numRows / ROW_PER_PAGE
	page, err := t.pager.ReadPage(pageNum)
	if err != nil {
		return err
	}

	rowNum := t.numRows - pageNum*ROW_PER_PAGE
	page.rows[rowNum] = row
	t.numRows = t.numRows + 1
	return nil
}

func (t *Table) Select() ([]*Row, error) {
	rows := []*Row{}
	for i := 0; i < t.numRows; i++ {
		pageNum := i / ROW_PER_PAGE
		rowIdx := i - pageNum*ROW_PER_PAGE
		page, err := t.pager.ReadPage(pageNum)
		if err != nil {
			return nil, err
		}
		row := page.Rows()[rowIdx]
		rows = append(rows, row)
	}
	return rows, nil
}

func (t *Table) NumRows() int {
	return t.numRows
}

func (t *Table) Pages() [TABLE_MAX_PAGES]*Page {
	return t.pages
}
