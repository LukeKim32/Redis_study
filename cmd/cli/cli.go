package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

	flags "github.com/jessevdk/go-flags"
)

type dataFlag struct {
	Key   string `short:"k" long:"key" description:"Key for (Key, Value) pair"`
	Value string `short:"v" long:"value" description:"Value or (Key, Value) pair"`
}

type clientFlag struct {
	Address string `short:"a" long:"addr" description:"IP(+port) address of Redis Node to be added"`
	Slave   bool   `short:"s" long:"slave" description:"If the passed address is slave client"`
	Master  bool   `short:"s" long:"slave" description:"If the passed address is master client"`
	SlaveOf string `long:"slaveOf" description:"IP(+port) address of Master Redis Node"`
}

const (
	/* constants for "index" of  */
	commandIdx = iota
	keyIdx
	valueIdx
)

const (
	Help = "help"
	Get  = "get"
	Set  = "set"
	Add  = "add"
	Ls   = "ls"
	List = "list"
	Exit = "exit"
	Quit = "quit"
)

func main() {

	stdReader := bufio.NewReader(os.Stdin)

	for {

		fmt.Print("hash-interface > ")
		inputText, err := stdReader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}

		words := strings.Fields(inputText)

		if len(words) == 0 {
			continue
		}

		switch words[commandIdx] {
		case Help:
			// print Instructions
		case Get:

			dataFlags := dataFlag{}
			if err := parseGetFlags(&dataFlags, words); err != nil {
				fmt.Println(err)
				continue
			}

			if err := requestGetToServer(dataFlags.Key); err != nil {
				fmt.Println(err)
				continue
			}

			break

		case Set:

			dataFlags := dataFlag{}
			if err := parseSetFlags(&dataFlags, words); err != nil {
				fmt.Println(err)
				continue
			}

			if err := requestSetToServer(dataFlags); err != nil {
				fmt.Println(err)
				continue
			}

			break

		case Add:
			clientFlags := clientFlag{}
			if err := parseClientFlags(&clientFlags, words); err != nil {
				fmt.Println(err)
				continue
			}

			if err := requestAddClientToServer(clientFlags); err != nil {
				fmt.Println(err)
				continue
			}

			break

		case List, Ls:
			if err := requestClientListToServer(); err != nil {
				fmt.Println(err)
				continue
			}

			break

		case Exit, Quit:
			return

		}
	}
}

func parseClientFlags(clientFlags *clientFlag, words []string) error {

	if _, err := flags.ParseArgs(clientFlags, words); err != nil {
		return err
	}

	if clientFlags.Address != "" {
		return fmt.Errorf("Add command must have 'Address' flag")
	}

	// 둘 중에 하나는 값이 true 여야 한다 (XOR true가 아닌 경우 에러)
	if !(clientFlags.Master != clientFlags.Slave) {
		return fmt.Errorf("Only one of Master Or Slave Flags must be set")
	}

	return nil
}

func parseGetFlags(dataFlags *dataFlag, words []string) error {

	if _, err := flags.ParseArgs(dataFlags, words); err != nil {
		return err
	}

	if dataFlags.Value != "" {
		return fmt.Errorf("Get command cannot have 'Value' flag")
	}

	if dataFlags.Key == "" {
		return fmt.Errorf("Get command must have 'Key' flag")
	}

	return nil
}

func parseSetFlags(dataFlags *dataFlag, words []string) error {

	if _, err := flags.ParseArgs(dataFlags, words); err != nil {
		return err
	}

	if dataFlags.Value == "" {
		return fmt.Errorf("Set command must have 'Value' flag")
	}

	if dataFlags.Key == "" {
		return fmt.Errorf("Set command must have 'Key' flag")
	}

	return nil
}
