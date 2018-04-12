package main

import (
	"github.com/tarantool/go-tarantool"
)

func (p *ProxyConnection) executeRequestCall(requestType uint32, requestID uint32,
	reader IprotoReader) (flags uint32, response *tarantool.Response, err error) {
	//|--------------- header ----------------|-----request_body -------|
	// <request_type><body_length><request_id> <flags><proc_name><tuple>
	var (
		cardinality uint32
		args        []interface{}
		param       interface{}
	)

	// parse flags
	err = unpackUint32(reader, &flags)
	if err != nil {
		return
	}

	// tarantool 1.5 "request call" always return tuple
	flags |= FlagReturnTuple

	// parse proc_name
	fieldLen, err := unpackUint64BER(reader, 64)
	if err != nil {
		return
	}
	procName := BytesToString(reader.Next(int(fieldLen)))

	// parse proc params tuple
	err = unpackUint32(reader, &cardinality)
	if err != nil {
		return
	}

	for fieldNo := uint32(0); fieldNo < cardinality; fieldNo++ {
		param, err = p.unpackFieldByDefs(reader, requestType, fieldNo, SchemaTypeStr)
		if err != nil {
			return
		}
		args = append(args, param)
	} //end for

	var tnt16 *tarantool.Connection
	if len(args) > 0 {
		tnt16 = p.getTnt16Master(args[0])
	} else {
		tnt16 = p.getTnt16Master(procName)
	}

	response, err = tnt16.Call(procName, args)
	p.statsdClient.Incr("call", 1)
	if err != nil {
		p.statsdClient.Incr("error_16", 1)
	}
	return
}
