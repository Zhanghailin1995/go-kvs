package main

import (
	"fmt"
	"kvs"
)

func main() {
	server, err := kvs.NewServer("./")
	if err != nil {
		fmt.Println(err)
		return
	}
	server.Run("tcp", "127.0.0.1:9019")
}
