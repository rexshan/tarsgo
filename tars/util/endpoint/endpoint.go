package endpoint

//Endpoint struct is used record a remote server instance.
type Endpoint struct {
	Host      string
	Port      int32
	IPPort    string // Host:Port
	Timeout   int32
	Istcp     int32 //need remove
	Proto     string
	Bind      string
	Container string
	SetId     string
}
