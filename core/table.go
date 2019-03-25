package core

import "fmt"

const COLUMN_USERNAME_LENGTH = 32
const COLUMN_EMAIL_LENGTH = 255

type Row struct {
	ID       int
	Username [COLUMN_USERNAME_LENGTH]rune
	Email    [COLUMN_EMAIL_LENGTH]rune
}

func (r *Row) String() string {
	username := string(r.Username[:COLUMN_USERNAME_LENGTH])
	email := string(r.Email[:COLUMN_EMAIL_LENGTH])
	return fmt.Sprintf("(%d, %s,%s)", r.ID, username, email)
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

const TABLE_MAX_PAGES = 100
const TABLE_MAX_ROWS = TABLE_MAX_PAGES * ROW_PER_PAGE

type Table struct {
	numRows int
	pages   [TABLE_MAX_PAGES]*Page
}

func (t *Table) InsertRow(row *Row) {
	pageNum := t.numRows / ROW_PER_PAGE
	page := t.pages[pageNum]
	if t.pages[pageNum] == nil {
		page = &Page{}
		t.pages[pageNum] = page
	}

	rowNum := t.numRows - pageNum*ROW_PER_PAGE
	page.rows[rowNum] = row
	t.numRows = t.numRows + 1
}

func (t *Table) NumRows() int {
	return t.numRows
}

func (t *Table) Pages() [TABLE_MAX_PAGES]*Page {
	return t.pages
}
