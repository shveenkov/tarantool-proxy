package main

import (
	"github.com/tarantool/go-tarantool"
)

func (p *ProxyConnection) executeRequestDelete(requestType uint32, requestID uint32,
	reader IprotoReader) (flags uint32, response *tarantool.Response, err error) {
	//|--------------- header ----------------|--------- body ---------|
	// <request_type><body_length><request_id> <space_no><flags><tuple>
	var (
		cardinality uint32
		spaceNo     uint32
		param       interface{}
		args        []interface{}
	)

	unpackUint32(reader, &spaceNo)     // parse space_no
	unpackUint32(reader, &flags)       // parse flags
	unpackUint32(reader, &cardinality) // parse insert tuple

	space, err := p.schema.GetSpaceInfo(spaceNo)
	if err != nil {
		return
	}

	// tnt15 delete default by indexNo=0
	indexName, err := space.GetIndexName(0)
	if err != nil {
		return
	}

	indexDefs, err := space.GetIndexDefs(0)
	if err != nil {
		return
	}

	for fieldNo := uint32(0); fieldNo < cardinality; fieldNo++ {
		param, err = p.unpackFieldByDefs(reader, requestType, fieldNo, indexDefs[fieldNo])
		if err != nil {
			return
		}
		args = append(args, param)
	} //end for

	tnt16 := p.getTnt16Master(args[0])

	response, err = tnt16.Delete(space.name, indexName, args)
	p.statsdClient.Incr("delete", 1)
	if err != nil {
		p.statsdClient.Incr("error_16", 1)
	}
	return
}
