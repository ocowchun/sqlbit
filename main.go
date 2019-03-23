package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
)

func printPrompt() {
	fmt.Print("> ")
}

func isMetaCommand(text string) bool {
	return text[0] == '.'
}

func runMetaCommand(text string) {
	if text == ".exit\n" {
		fmt.Println("bye")
		os.Exit(0)
	} else {
		fmt.Printf("Unrecognized command %s", text)
	}
}

type Statement struct {
	Type string
}

// our SQL Compilier
func prepareStatement(text string) (Statement, error) {
	if strings.HasPrefix(text, "insert") {
		return Statement{"insert"}, nil
	}
	if strings.HasPrefix(text, "select") {
		return Statement{"select"}, nil
	}
	if strings.HasPrefix(text, "delete") {
		return Statement{"delete"}, nil
	}
	return Statement{}, errors.New("UNRECOGNIZED_STATEMENT")
}

func runStatement(statement Statement) {
	fmt.Printf("Run %s command\n", statement.Type)
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Welcome to sqlbit 0.0.1\n")
	for true {

		printPrompt()
		text, _ := reader.ReadString('\n')
		if isMetaCommand(text) {
			runMetaCommand(text)
		} else {
			statement, err := prepareStatement(text)
			if err != nil {
				fmt.Println(err)
			} else {
				runStatement(statement)
			}
		}
	}
}
