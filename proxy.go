package main

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"math/rand"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"unsafe"

	"github.com/quipo/statsd"
	"github.com/tarantool/go-tarantool"
)

//ProxyConnection control struct
type ProxyConnection struct {
	reader       *bufio.Reader
	writer       *bufio.Writer
	chanSem      chan struct{}  // mutex for limit goroutines count request process
	chanResponse chan *Response // channel for response write from tarantool 1.6
	chanCntl     chan struct{}  // control chan for connection close
	schema       *Schema
	tntPool      [][]*tarantool.Connection
	tntPoolLen   uint32
	listenNum    int           // sharding data
	statsdClient statsd.Statsd // statsd client for send metrics
}

// Response iproto package
type Response struct {
	RequestType uint32
	RequestID   uint32
	Body        []byte
}

// Tnt15Executor iproto request executor func
type Tnt15Executor func(uint32, uint32, IprotoReader) (flags uint32, response *tarantool.Response, err error)

const maxPoolCap = 2048

var bytesBufferPool = sync.Pool{
	New: func() interface{} { return bytes.NewBuffer(make([]byte, 0, 64)) },
}

func getBytesBufferFromPool() (b *bytes.Buffer) {
	ifc := bytesBufferPool.Get()
	if ifc != nil {
		b = ifc.(*bytes.Buffer)
	}
	return
}

func putBytesBufferToPool(b *bytes.Buffer) {
	if b.Cap() <= maxPoolCap {
		b.Reset()
		bytesBufferPool.Put(b)
	}
}

func newProxyConnection(conn io.ReadWriteCloser, listenNum int, tntPool [][]*tarantool.Connection, schema *Schema, statsdClient statsd.Statsd) *ProxyConnection {
	proxy := &ProxyConnection{
		reader: bufio.NewReaderSize(conn, 8*1024),
		writer: bufio.NewWriterSize(conn, 8*1024),

		chanSem:      make(chan struct{}, 128),
		chanResponse: make(chan *Response, 128),
		chanCntl:     make(chan struct{}),

		schema: schema,

		tntPool:      tntPool,
		tntPoolLen:   uint32(len(tntPool)),
		listenNum:    listenNum,
		statsdClient: statsdClient,
	}

	return proxy
}

func (p *ProxyConnection) getShardNum(shardingKey interface{}, lenPool uint32) uint32 {
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	v := reflect.ValueOf(shardingKey)
	switch v.Kind() {
	case reflect.String:
		buf.WriteString(v.String())
	case reflect.Uint, reflect.Uint8, reflect.Uint16,
		reflect.Uint32, reflect.Uint64:
		buf.WriteString(strconv.FormatUint(v.Uint(), 10))
	case reflect.Int, reflect.Int8, reflect.Int16,
		reflect.Int32, reflect.Int64:
		buf.WriteString(strconv.FormatInt(v.Int(), 10))
	default:
		log.Printf("Error getShardNum for shardingKey: %v(%T)", v, v)
		p.statsdClient.Incr("error_15", 1)
		return 0
	}
	shardNum := crc32.ChecksumIEEE(buf.Bytes()) % lenPool
	return shardNum
}

func (p *ProxyConnection) getTnt16Pool(shardingKey interface{}) []*tarantool.Connection {
	if !p.schema.shardingEnabled {
		shardNum := uint32(p.listenNum)
		return p.tntPool[shardNum]
	}

	shardNum := p.getShardNum(shardingKey, p.tntPoolLen)
	return p.tntPool[shardNum]
}

func (p *ProxyConnection) getTnt16Master(shardingKey interface{}) *tarantool.Connection {
	return p.getTnt16Pool(shardingKey)[0]
}

func (p *ProxyConnection) getTnt16(shardingKey interface{}) *tarantool.Connection {
	pool := p.getTnt16Pool(shardingKey)
	return pool[rand.Intn(len(pool))]
}

func (p *ProxyConnection) packTnt16Error(body IprotoWriter, errMsg error) {
	PackUint32(body, (ErrorUnpackData<<8)|BadResponse15Status)
	errStr := errMsg.Error()
	body.WriteString(errStr)
}

func (p *ProxyConnection) packTnt16ResponseBody(body IprotoWriter, response *tarantool.Response, flags uint32) {
	if response.Code != 0 {
		PackUint32(body, (response.Code<<8)|BadResponse15Status)
		body.WriteString(response.Error)
		log.Printf("error response16 code=%d %s", response.Code, response.Error)
		p.statsdClient.Incr("error_16", 1)
		return
	}

	// ping request nodata
	if flags&FlagPing != 0 {
		return
	}

	count := uint32(len(response.Data))
	if flags&FlagReturnTuple == 0 {
		count = 0
	}
	PackUint32(body, response.Code)
	PackUint32(body, count)

	if count == 0 {
		return
	}

	tupleBuf := getBytesBufferFromPool()
	for _, tuple := range response.Data {
		tupleBuf.Reset()
		tupleIter, ok := tuple.([]interface{})
		if !ok {
			tupleIter = []interface{}{tuple}
		}
		for _, iValue := range tupleIter {
			switch val := iValue.(type) {
			case uint32:
				packUint64BER(tupleBuf, 4)
				PackUint32(tupleBuf, uint32(val))
			case uint64:
				packUint64BER(tupleBuf, 4)
				PackUint32(tupleBuf, uint32(val))
			case int64:
				packUint64BER(tupleBuf, 4)
				PackUint32(tupleBuf, uint32(val))
			case string:
				packUint64BER(tupleBuf, uint64(len(val)))
				tupleBuf.WriteString(val)
			default:
				// return nil, fmt.Errorf("error pack value: %v(%T)", val, val)
				log.Printf("error pack value: %v(%T)", val, val)
				tupleBuf.WriteString("")
			} //end switch*/
		} //end for

		PackUint32(body, uint32(tupleBuf.Len()))
		// add cardinality
		PackUint32(body, uint32(len(tupleIter)))

		io.Copy(body, tupleBuf)
	} //end for
	putBytesBufferToPool(tupleBuf)
	return
}

func (p *ProxyConnection) tarantool15SendResponse() {
	header := &bytes.Buffer{}
	var response *Response

FOR_CLIENT_POLL:
	for {
		select {
		case response = <-p.chanResponse:
		default:
			runtime.Gosched()

			if len(p.chanResponse) == 0 {
				if err := p.writer.Flush(); err != nil {
					log.Println("client close connection")
					p.statsdClient.Incr("error_15", 1)
					break FOR_CLIENT_POLL
				} //end if
			}

			select {
			case response = <-p.chanResponse:
			case <-p.chanCntl:
				break FOR_CLIENT_POLL
			} //end select
		} //end select

		header.Reset()

		PackUint32(header, response.RequestType)
		PackUint32(header, uint32(len(response.Body)))
		PackUint32(header, response.RequestID)

		if _, err := p.writer.Write(header.Bytes()); err != nil {
			log.Printf("error write response header: %s", err)
			p.statsdClient.Incr("error_15", 1)
		}

		if _, err := p.writer.Write(response.Body); err != nil {
			log.Printf("error write response body: %s", err)
			p.statsdClient.Incr("error_15", 1)
		}
		putBytesBufferToPool(bytes.NewBuffer(response.Body))
	} //end for

	// p.writer.Flush()
}

func (p *ProxyConnection) processIproto() {
	// bg response process for 15
	go p.tarantool15SendResponse()

	// https://github.com/tarantool/tarantool/blob/stable/doc/box-protocol.txt
	mapCall := map[uint32]Tnt15Executor{
		RequestTypeSelect: p.executeRequestSelect,
		RequestTypeCall:   p.executeRequestCall,
		RequestTypeInsert: p.executeRequestInsert,
		RequestTypeDelete: p.executeRequestDelete,
		RequestTypeUpdate: p.executeRequestUpdate,
		RequestTypePing:   p.executeRequestPing,
	}

	// shared buffer for parse header
	iprotoHeader := bytes.NewBuffer(make([]byte, 4))
	for {
		var (
			requestType uint32
			bodyLength  uint32
			requestID   uint32
		)

		//read iproto header
		iprotoHeader.Reset()
		_, err := io.CopyN(iprotoHeader, p.reader, 12)
		if err != nil {
			if err != io.EOF {
				log.Printf("error read header15: - %s", err)
				p.statsdClient.Incr("error_15", 1)
			}
			break
		}

		unpackUint32(iprotoHeader, &requestType)
		unpackUint32(iprotoHeader, &bodyLength)
		unpackUint32(iprotoHeader, &requestID)

		//read iproto body
		iprotoPackage := getBytesBufferFromPool()
		_, err = io.CopyN(iprotoPackage, p.reader, int64(bodyLength))
		if err != nil {
			if err != io.EOF {
				log.Printf("error read body15: - %s", err)
				p.statsdClient.Incr("error_15", 1)
			}
			break
		}

		requestExecutor, ok := mapCall[requestType]
		if !ok {
			err := fmt.Errorf("unknown request type15: %d", requestType)
			body := getBytesBufferFromPool()
			p.packTnt16Error(body, err)

			response := &Response{
				RequestType: requestType,
				RequestID:   requestID,
				Body:        body.Bytes(),
			}

			p.chanResponse <- response
			continue
		}

		p.chanSem <- struct{}{}
		go func() {
			flags, tnt16Response, err := requestExecutor(requestType, requestID, iprotoPackage)
			putBytesBufferToPool(iprotoPackage)

			body := getBytesBufferFromPool()
			if err != nil {
				p.packTnt16Error(body, err)
			} else {
				p.packTnt16ResponseBody(body, tnt16Response, flags)
			}

			response := &Response{
				RequestType: requestType,
				RequestID:   requestID,
				Body:        body.Bytes(),
			}
			select {
			case p.chanResponse <- response:
			default:
				select {
				case p.chanResponse <- response:
				case <-p.chanCntl:
				}
			}

			<-p.chanSem
		}()
	} //end for

	// close control chan for stop tarantool15SendResponse
	close(p.chanCntl)
}

// BytesToString without alloc
func BytesToString(b []byte) string {
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh := reflect.StringHeader{bh.Data, bh.Len}
	return *(*string)(unsafe.Pointer(&sh))
}

func (p *ProxyConnection) unpackFieldByDefs(reader IprotoReader, requestType, fieldNo uint32, fieldType string) (val interface{}, err error) {
	fieldLen, err := unpackUint64BER(reader, 64)
	if err != nil {
		return
	}

	fieldSlice := reader.Next(int(fieldLen))
	switch fieldType {
	case SchemaTypeInt:
		if fieldLen != 4 {
			err = fmt.Errorf("error unpackUint32 for requestType: %d; fieldNo: %d ", requestType, fieldNo)
			return
		}
		val = unpackUint32Bytes(fieldSlice)
	case SchemaTypeInt64:
		if fieldLen != 8 {
			err = fmt.Errorf("error unpackUint64 for requestType: %d; fieldNo: %d ", requestType, fieldNo)
			return
		}
		val = unpackUint64Bytes(fieldSlice)
	case SchemaTypeStr:
		val = BytesToString(fieldSlice)
	default:
		val = BytesToString(fieldSlice)
	} //end switch

	return
}
