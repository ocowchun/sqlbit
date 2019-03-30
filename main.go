package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/ocowchun/sqlbit/core"
	"github.com/ocowchun/sqlbit/statement"
)

func printPrompt() {
	fmt.Print("> ")
}

func isMetaCommand(text string) bool {
	return text[0] == '.'
}

func runMetaCommand(text string, table *core.Table) {
	if text == ".exit" {
		fmt.Println("bye")
		table.CloseTable()
		os.Exit(0)
	} else {
		fmt.Printf("Unrecognized command %s", text)
	}
}

// our SQL Compilier
func prepareStatement(text string) (statement.Statement, error) {
	if strings.HasPrefix(text, "insert") {
		return statement.PrepareInsert(text)
	}
	if strings.HasPrefix(text, "select") {
		return statement.Statement{statement.StatementType_Select, nil}, nil
	}
	if strings.HasPrefix(text, "delete") {
		return statement.Statement{statement.StatementType_Delete, nil}, nil
	}
	return statement.Statement{}, errors.New("UNRECOGNIZED_STATEMENT")
}

func executeStatement(s statement.Statement, table *core.Table) {
	switch statementType := s.Type; statementType {
	case statement.StatementType_Insert:
		statement.ExecuteInsert(s, table)
	case statement.StatementType_Select:
		statement.ExecuteSelect(s, table)
	}
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Welcome to sqlbit 0.0.1\n")
	table, err := core.OpenTable("/Users/ocowchun/go/src/github.com/ocowchun/sqlbit/tmp/test.db")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	for true {
		printPrompt()
		text, _ := reader.ReadString('\n')
		text = text[:len(text)-1]
		if isMetaCommand(text) {
			runMetaCommand(text, table)
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
