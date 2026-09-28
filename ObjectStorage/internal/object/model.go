package object

import "time"

type Metadata struct {
	Bucket 		string  `json:"bucket"`
	Key 		string  `json:"key"`
	Size 		int64   `json:"size"`
	ContentType string  `json:"content_type"`
	ETag 		string  `json:"etag"`
	CreatedAt 	time.Time `json:"created_at"`
}