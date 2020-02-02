package configs

const (
	// HTTP protocol
	HTTP = "http://"
	// HTTPS protocol
	HTTPS = "https://"
	// BaseURL consists of IP address + Port + specific path
	BaseURL = "localhost/interface"
	// Port is 8080
	Port = 8888
	// JSONContent is for response header
	JSONContent = "application/json"
	// CORSheader is a header field for Cross Origin Resource Sharing Problem Solve
	CORSheader     = "Access-Control-Allow-Origin"
	ContentType    = "Content-Type"
	BadRequestBody = "Unappropriate Request Body"
)

// CurrentIP is IP address of Go-application, will be initialized in main.go
var CurrentIP string