package main

import (
	"github.com/tarantool/go-tarantool"
)

func (p *ProxyConnection) executeRequestSelect(requestType uint32, requestID uint32,
	reader IprotoReader) (flags uint32, response *tarantool.Response, err error) {
	//|--------------- header ----------------|---------------request_body ---------------------...|
	// <request_type><body_length><request_id> <space_no><index_no><offset><limit><count><tuple>+
	var (
		spaceNo     uint32
		indexNo     uint32
		offset      uint32
		limit       uint32
		count       uint32
		cardinality uint32
		args        []interface{}
		param       interface{}
	)

	unpackUint32(reader, &spaceNo)
	unpackUint32(reader, &indexNo)
	unpackUint32(reader, &offset)
	unpackUint32(reader, &limit)
	unpackUint32(reader, &count)

	flags = FlagReturnTuple

	space, err := p.schema.GetSpaceInfo(spaceNo)
	if err != nil {
		return
	}

	indexName, err := space.GetIndexName(indexNo)
	if err != nil {
		return
	}

	indexDefs, err := space.GetIndexDefs(indexNo)
	if err != nil {
		return
	}

	// далее лежат упакованные iproto tuple-ы
	for i := uint32(0); i < count; i++ {
		err = unpackUint32(reader, &cardinality)
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
	} //end for

	//sharding for key0
	tnt16 := p.getTnt16(args[0])
	response, err = tnt16.Select(space.name, indexName, offset, limit, tarantool.IterEq, args)
	if err == nil {
		p.statsdClient.Incr("select", 1)
		return
	}
	p.statsdClient.Incr("error_16", 1)

	// make fault tollerance requests
	for _, tnt16i := range p.getTnt16Pool(args[0]) {
		if tnt16i == tnt16 {
			continue
		}

		response, err = tnt16i.Select(space.name, indexName, offset, limit, tarantool.IterEq, args)
		if err == nil {
			p.statsdClient.Incr("select", 1)
			break
		}
		p.statsdClient.Incr("error_16", 1)
	}
	return
}
