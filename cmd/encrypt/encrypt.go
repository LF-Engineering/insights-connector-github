package main

import (
	"fmt"
	"os"

	"github.com/LF-Engineering/insights-datasource-shared/cryptography"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("error: you need to specify data to encrypt\n")
		os.Exit(1)
	}
	encrypt, err := cryptography.NewEncryptionClient()
	if err != nil {
		fmt.Printf("error: %+v\n", err)
		os.Exit(2)
	}
	encrypted, err := encrypt.Encrypt(os.Args[1])
	if err != nil {
		fmt.Printf("error: %+v\n", err)
		os.Exit(3)
	}
	fmt.Printf("%s", encrypted)
}
