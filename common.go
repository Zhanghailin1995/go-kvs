package kvs

type RequestType uint16

const (
	GetReq RequestType = iota
	SetReq
	RemoveReq
)

type Request struct {
	Type  RequestType
	Key   string
	value string
}

type GetResponse struct {
	Ok  bool
	Res string
	Err string
}

type SetResponse struct {
	Ok  bool
	Err string
}

type RemoveResponse struct {
	Ok  bool
	Err string
}
