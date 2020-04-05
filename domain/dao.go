package domain

import "encoding/json"

//can move this to domain
type Request struct {
	UID string `json:"uid"`
	Method string `json:"method"`
	Topic string `json:"topic"`
	FwdTopic string `json:"fwd_topic"`
	Data  json.RawMessage `json:"data"'`
}
//can move this to domain
type Response struct {
	Data json.RawMessage `json:"data"`
	UID  string  `json:"uid"`
}
