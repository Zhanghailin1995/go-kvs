package main

import (
	"fmt"
	"kvs"
)

func main() {
	client, _ := kvs.Connect("tcp", "127.0.0.1:9019")

	//for i := 0; i < 100; i++ {
	//	key := fmt.Sprintf("key%d", i)
	//	value := strconv.Itoa(i) + "c1"
	//	client.Set(key, value)
	//}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		v, _ := client.Get(key)
		fmt.Println(key, " : ", v)
	}

	client1, _ := kvs.Connect("tcp", "127.0.0.1:9019")

	//for i := 0; i < 100; i++ {
	//	key := fmt.Sprintf("key%d", i)
	//	value := strconv.Itoa(i) + "c2"
	//	client1.Set(key, value)
	//}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		v, _ := client1.Get(key)
		fmt.Println(key, " : ", v)
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		v, _ := client.Get(key)
		fmt.Println(key, " : ", v)
	}
}
