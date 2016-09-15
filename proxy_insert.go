package main

import (
	"github.com/tarantool/go-tarantool"
)

func (self *ProxyConnection) executeRequestInsert(requestType uint32, requestId uint32,
	reader IprotoReader) (flags uint32, response *tarantool.Response, err error) {
	//|--------------- header ----------------|--------- body ---------|
	// <request_type><body_length><request_id> <space_no><flags><tuple>
	var (
		cardinality uint32
		spaceNo     uint32
		args        []interface{}
		param       interface{}
	)

	unpackUint32(reader, &spaceNo)     // parse space_no
	unpackUint32(reader, &flags)       // parse flags
	unpackUint32(reader, &cardinality) // parse insert tuple

	space, err := self.schema.GetSpaceInfo(spaceNo)
	if err != nil {
		return
	}
	fieldDefs := space.typeFieldsMap

	for fieldNo := uint32(0); fieldNo < cardinality; fieldNo += 1 {
		param, err = self.unpackFieldByDefs(reader, requestType, fieldNo, fieldDefs[fieldNo])
		if err != nil {
			return
		}
		args = append(args, param)
	} //end for

	tnt16 := self.getTnt16Master(args[0])
	switch {
	case flags&FlagAdd != 0:
		response, err = tnt16.Insert(space.name, args)
	case flags&FlagReplace != 0:
		response, err = tnt16.Replace(space.name, args)
	default:
		// use upsert default
		var upsert_args []interface{}
		for fieldNo, fieldVal := range args {
			upsert_args = append(upsert_args, []interface{}{"=", fieldNo, fieldVal})
		}
		response, err = tnt16.Upsert(space.name, args, upsert_args)
	}
	return
}
