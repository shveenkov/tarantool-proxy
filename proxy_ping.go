package main

import (
	"github.com/tarantool/go-tarantool"
)

func (p *ProxyConnection) executeRequestPing(requestType uint32, requestID uint32,
	reader IprotoReader) (flags uint32, response *tarantool.Response, err error) {
	//Ping body is empty, so body_length == 0 and there's no body
	//|--------------- header ----------------|
	// <request_type><body_length><request_id>
	flags = FlagPing

	tnt16 := p.getTnt16Master("ping")
	response, err = tnt16.Ping()
	p.statsdClient.Incr("ping", 1)
	if err != nil {
		p.statsdClient.Incr("error_16", 1)
	}
	return
}
