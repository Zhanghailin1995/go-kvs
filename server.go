package kvs

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
)

type innerKvsServer struct {
}

// KvsServer The server of a key value store.
type KvsServer struct {
	engine KvsEngine
}

func NewServer(path string) (*KvsServer, error) {
	store, err := Open("./")
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	server := &KvsServer{
		engine: store,
	}
	return server, nil
}

func (k *KvsServer) Run(network, addr string) error {
	listen, err := net.Listen(network, addr)
	if err != nil {
		return err
	}

	for {
		conn, err := listen.Accept()
		if err != nil {
			fmt.Println("accept failed, err:", err)
			continue
		}
		engine := k.engine.clone()
		go process(engine, conn)
	}
}

func process(engine KvsEngine, conn net.Conn) {
	defer conn.Close()
	defer engine.shutdown()
	tcpStreamReader := bufio.NewReader(conn)
	tcpStreamWriter := bufio.NewWriter(conn)

	dec := json.NewDecoder(tcpStreamReader)

	for {
		var req Request
		if err := dec.Decode(&req); err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("unexpect error occur.", err)
			break
		} else {
			switch req.Type {
			case GetReq:
				var res GetResponse
				v, err := engine.Get(req.Key)
				if err != nil {
					res = GetResponse{
						false,
						"",
						err.Error(),
					}
				} else {
					res = GetResponse{
						true,
						v,
						"",
					}
				}
				err = sendResponse(tcpStreamWriter, &res)
				if err != nil {
					fmt.Println("send response error.", err)
				}
				break
			case SetReq:
				var res SetResponse
				err := engine.Set(req.Key, req.Value)
				if err != nil {
					res = SetResponse{
						false,
						err.Error(),
					}
				} else {
					res = SetResponse{
						true,
						"",
					}
				}
				err = sendResponse(tcpStreamWriter, &res)
				if err != nil {
					fmt.Println("send response error.", err)
				}
				break
			case RemoveReq:
				var res RemoveResponse
				err := engine.Remove(req.Key)
				if err != nil {
					res = RemoveResponse{
						false,
						err.Error(),
					}
				} else {
					res = RemoveResponse{
						true,
						"",
					}
				}
				err = sendResponse(tcpStreamWriter, &res)
				if err != nil {
					fmt.Println("send response error.", err)
				}
				break
			}
		}
	}
}

func sendResponse(w *bufio.Writer, v interface{}) error {
	enc := json.NewEncoder(w)
	err := enc.Encode(v)
	if err != nil {
		return err
	}
	err = w.Flush()
	if err != nil {
		return err
	}
	return nil
}
