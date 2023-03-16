package twitter

import (
	"github.com/linkedin/goavro/v2"
)

var AvroSchema string = `
{
	"type": "record",
	"name": "Event",
	"fields": [
	  {
		"name": "data",
		"type": "record",
		"fields": [
		  { "name": "id", "type": "string" },
		  { "name": "text", "type": "string" },
		  { "name": "lang", "type": "string" },
		  { "name": "created_at", "type": "string" }
		]
	  },
	  {
		"name": "author",
		"type": "record",
		"fields": [
		  { "name": "id", "type": "string" },
		  { "name": "name", "type": "string" },
		  { "name": "username", "type": "string" },
		  { "name": "created_at", "type": "string" },
		  { "name": "followers", "type": "long" }
		]
	  }
	]
}   
`

var AvroCodec *goavro.Codec = nil

func init() {
	var err error
	AvroCodec, err = goavro.NewCodec(AvroSchema)
	if err != nil {
		panic(err)
	}
}
