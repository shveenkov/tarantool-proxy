package main

import (
	"fmt"
	"github.com/tarantool/go-tarantool"
)

func (self *ProxyConnection) executeRequestUpdate(requestType uint32, requestId uint32,
	reader IprotoReader) (flags uint32, response *tarantool.Response, err error) {
	// |--------------- header ----------------|---------------request_body --------------...|
	//  <request_type><body_length><request_id> <space_no><flags><tuple><count><operation>+
	//                                                            |      |      |
	//                        Key to search in primary index -----+      |      +-- list of operations
	//                        (tuple with cardinality=1)                 +-- number of operations
	var (
		cardinality uint32
		spaceNo     uint32
		opCount     uint32
		opCode      uint8
		args        []interface{}
		param       interface{}
		fieldNo     uint32
		keyTuple    []interface{}
	)

	unpackUint32(reader, &spaceNo) // parse space_no
	unpackUint32(reader, &flags)   // parse flags

	space, err := self.schema.GetSpaceInfo(spaceNo)
	if err != nil {
		return
	}
	fieldDefs := space.typeFieldsMap

	// tnt15 update default by indexNo=0 primary
	indexName, err := space.GetIndexName(0)
	if err != nil {
		return
	}

	indexDefs, err := space.GetIndexDefs(0)
	if err != nil {
		return
	}

	unpackUint32(reader, &cardinality) // parse key tuple

	for fieldNo := uint32(0); fieldNo < cardinality; fieldNo += 1 {
		param, err = self.unpackFieldByDefs(reader, requestType, fieldNo, indexDefs[fieldNo])
		if err != nil {
			return
		}
		keyTuple = append(keyTuple, param)
	} //end for

	//parse op count
	unpackUint32(reader, &opCount)

	//parse op list
	for opNo := uint32(0); opNo < opCount; opNo += 1 {
		err = unpackUint32(reader, &fieldNo)
		if err != nil {
			return
		}

		err = unpackUint8(reader, &opCode)
		if err != nil {
			return
		}

		opSymbol, ok := UpdateOperationCode[opCode]
		if !ok {
			err = fmt.Errorf("error operation code: %d for update request", opCode)
			return
		}

		param, err = self.unpackFieldByDefs(reader, requestType, fieldNo, fieldDefs[fieldNo])
		if err != nil {
			return
		}
		args = append(args, []interface{}{opSymbol, fieldNo, param})
	} //end for opCount

	tnt16 := self.getTnt16Master(keyTuple[0])

	response, err = tnt16.Update(space.name, indexName, keyTuple, args)
	return
}
