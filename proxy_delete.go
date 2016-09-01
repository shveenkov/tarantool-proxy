package main

import (
	"github.com/tarantool/go-tarantool"
)

func (self *ProxyConnection) executeRequestDelete(requestType uint32, requestId uint32,
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

	space, err := self.schema.GetSpaceInfo(spaceNo)
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

	for fieldNo := uint32(0); fieldNo < cardinality; fieldNo += 1 {
		param, err = self.unpackFieldByDefs(reader, requestType, fieldNo, indexDefs[fieldNo])
		if err != nil {
			return
		}
		args = append(args, param)
	} //end for

	tnt16 := self.getTnt16Master(args[0])

	response, err = tnt16.Delete(space.name, indexName, args)
	return
}
