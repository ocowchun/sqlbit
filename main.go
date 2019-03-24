package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func printPrompt() {
	fmt.Print("> ")
}

func isMetaCommand(text string) bool {
	return text[0] == '.'
}

func runMetaCommand(text string) {
	if text == ".exit" {
		fmt.Println("bye")
		os.Exit(0)
	} else {
		fmt.Printf("Unrecognized command %s", text)
	}
}

type StatementType int

const (
	StatementType_Insert StatementType = iota
	StatementType_Select
	StatementType_Delete
)

type Statement struct {
	Type        StatementType
	RowToInsert *Row
}

const PAGE_SIZE = 4096

// 4 + 32 + 255
const ROW_SIZE = 291
const ROW_PER_PAGE = PAGE_SIZE / ROW_SIZE

type Page struct {
	rows [ROW_PER_PAGE]*Row
}

const TABLE_MAX_PAGES = 100
const TABLE_MAX_ROWS = TABLE_MAX_PAGES * ROW_PER_PAGE

type Table struct {
	numRows int
	pages   [TABLE_MAX_PAGES]*Page
}

func (t *Table) insertRow(row *Row) {
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

const COLUMN_USERNAME_LENGTH = 32
const COLUMN_EMAIL_LENGTH = 255

type Row struct {
	id       int
	username [COLUMN_USERNAME_LENGTH]rune
	email    [COLUMN_EMAIL_LENGTH]rune
}

func (r *Row) String() string {
	username := string(r.username[:COLUMN_USERNAME_LENGTH])
	email := string(r.email[:COLUMN_EMAIL_LENGTH])
	return fmt.Sprintf("(%d, %s,%s)", r.id, username, email)
}

func extractUserFromTokens(tokens []string) (Statement, error) {
	idIdx := 1
	usernameIdx := 2
	emailIdx := 3

	userID, err := strconv.Atoi(tokens[idIdx])
	if err != nil {
		return Statement{}, errors.New("id must be integer")
	}
	var username [COLUMN_USERNAME_LENGTH]rune
	copy(username[:], []rune(tokens[usernameIdx]))

	var email [COLUMN_EMAIL_LENGTH]rune
	copy(email[:], []rune(tokens[emailIdx]))

	row := &Row{userID, username, email}
	return Statement{StatementType_Insert, row}, nil
}

// our SQL Compilier
func prepareStatement(text string) (Statement, error) {
	if strings.HasPrefix(text, "insert") {
		tokens := strings.Split(text, " ")
		if len(tokens) != 4 {
			return Statement{}, errors.New("PREPARE_SYNTAX_ERROR")
		}
		return extractUserFromTokens(tokens)
	}
	if strings.HasPrefix(text, "select") {
		return Statement{StatementType_Select, nil}, nil
	}
	if strings.HasPrefix(text, "delete") {
		return Statement{StatementType_Delete, nil}, nil
	}
	return Statement{}, errors.New("UNRECOGNIZED_STATEMENT")
}

type ExecuteResult int

const (
	ExecuteResult_Success ExecuteResult = iota
	ExecuteResult_TableFull
)

func executeInsert(statement Statement, table *Table) ExecuteResult {
	if table.numRows >= TABLE_MAX_ROWS {
		return ExecuteResult_TableFull
	}

	table.insertRow(statement.RowToInsert)
	return ExecuteResult_Success
}

func executeSelect(statement Statement, table *Table) ExecuteResult {
	numRows := table.numRows
	for i := 0; i < numRows; i++ {
		pageIdx := i / ROW_PER_PAGE
		rowIdx := i - pageIdx*ROW_PER_PAGE
		row := table.pages[pageIdx].rows[rowIdx]
		fmt.Println(row)
	}
	return ExecuteResult_Success
}

func executeStatement(statement Statement, table *Table) {
	switch statementType := statement.Type; statementType {
	case StatementType_Insert:
		executeInsert(statement, table)
	case StatementType_Select:
		executeSelect(statement, table)
	}
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Welcome to sqlbit 0.0.1\n")
	table := &Table{}
	for true {
		printPrompt()
		text, _ := reader.ReadString('\n')
		text = text[:len(text)-1]
		if isMetaCommand(text) {
			runMetaCommand(text)
		} else {
			statement, err := prepareStatement(text)
			if err != nil {
				fmt.Println(err)
			} else {
				executeStatement(statement, table)
			}
		}
	}
}
