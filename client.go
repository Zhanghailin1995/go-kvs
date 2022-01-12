package kvs

import (
	"bufio"
	"encoding/json"
	"errors"
	"kvs/utils/log"
	"net"
)

type KvsClient struct {
	writer *bufio.Writer
	reader *json.Decoder
}

func Connect(network, addr string) (*KvsClient, error) {
	log.Infof("connecting to [%s]", addr)
	conn, err := net.Dial(network, addr)
	if err != nil {
		log.Errorf("connect to [%s] error, %v", addr, err)
		return nil, err
	}
	log.Infof("connect to [%s] success", addr)
	reader := json.NewDecoder(bufio.NewReader(conn))
	writer := bufio.NewWriter(conn)
	return &KvsClient{writer, reader}, nil
}

func (c *KvsClient) Get(key string) (string, error) {
	req := Request{
		GetReq, key, "",
	}
	err := sendReq(c.writer, &req)
	if err != nil {
		return "", err
	}
	var res GetResponse
	err = c.reader.Decode(&res)
	if err != nil {
		return "", err
	}
	if res.Ok {
		return res.Res, nil
	} else {
		return "", errors.New(res.Err)
	}
}

func (c *KvsClient) Set(key string, value string) error {
	req := Request{
		SetReq, key, value,
	}
	err := sendReq(c.writer, &req)
	if err != nil {
		return err
	}
	var res SetResponse
	err = c.reader.Decode(&res)
	if err != nil {
		return err
	}
	if res.Ok {
		return nil
	} else {
		return errors.New(res.Err)
	}
}

func (c *KvsClient) Remove(key string) error {
	req := Request{
		RemoveReq, key, "",
	}
	err := sendReq(c.writer, &req)
	if err != nil {
		return err
	}
	var res RemoveResponse
	err = c.reader.Decode(&res)
	if err != nil {
		return err
	}
	if res.Ok {
		return nil
	} else {
		return errors.New(res.Err)
	}
}

func sendReq(w *bufio.Writer, v interface{}) error {
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
