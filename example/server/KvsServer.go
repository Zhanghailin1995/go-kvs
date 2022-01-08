package main

import (
	"fmt"
	"kvs"
)

func main() {
	store, err := kvs.Open("./")
	if err != nil {
		fmt.Println(err)
		return
	}
	server := kvs.KvsServer{
		Engine: store,
	}

	server.Run("tcp", "127.0.0.1:9019")
}
