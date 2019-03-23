package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Welcome to sqlbit 0.0.1\n")
	for true {
		fmt.Print("> ")
		text, _ := reader.ReadString('\n')
		fmt.Println(text)
		if text == ".exit\n" {
			fmt.Println("bye")
			os.Exit(0)
		}
	}
}
