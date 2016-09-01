package main

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/tarantool/go-tarantool"
	"hash/crc32"
	"io"
	"log"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"unsafe"
)

type ProxyConnection struct {
	reader       *bufio.Reader
	writer       *bufio.Writer
	chanSem      chan struct{}  // mutex for limit goroutines count request process
	chanResponse chan *Response // channel for response write from tarantool 1.6
	chanCntl     chan struct{}  // control chan for connection close
	schema       *Schema
	tntPool      [][]*tarantool.Connection
	tntPoolLen   uint32

	// sharding data
	listenNum        int
	shardCounter     []int
	chanShardCounter chan *ShardCounterFuture
}

type ShardCounterFuture struct {
	shardNum   uint32
	shardIndex int
	lenPool    int
	ready      chan struct{}
}

type Response struct {
	RequestType uint32
	RequestId   uint32
	Body        []byte
}

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

func newProxyConnection(conn io.ReadWriteCloser, listenNum int, tntPool [][]*tarantool.Connection, schema *Schema) *ProxyConnection {
	proxy := &ProxyConnection{
		reader: bufio.NewReaderSize(conn, 8*1024),
		writer: bufio.NewWriterSize(conn, 8*1024),

		chanSem:      make(chan struct{}, 10000),
		chanResponse: make(chan *Response, 100000),
		chanCntl:     make(chan struct{}),

		schema: schema,

		tntPool:          tntPool,
		tntPoolLen:       uint32(len(tntPool)),
		listenNum:        listenNum,
		shardCounter:     make([]int, len(tntPool)),
		chanShardCounter: make(chan *ShardCounterFuture),
	}

	return proxy
}

func getShardNum(shardingKey interface{}, lenPool uint32) uint32 {
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
		return 0
	}
	shardNum := crc32.ChecksumIEEE(buf.Bytes()) % lenPool
	return shardNum
}

func (self *ProxyConnection) getShardNumIndex(shardNum uint32, lenPool int) int {
	future := &ShardCounterFuture{shardNum, 0, lenPool, make(chan struct{})}
	self.chanShardCounter <- future
	<-future.ready
	return future.shardIndex
}

func (self *ProxyConnection) getTnt16Master(shardingKey interface{}) *tarantool.Connection {
	if !self.schema.shardingEnabled {
		return self.tntPool[self.listenNum][0]
	}

	shardNum := getShardNum(shardingKey, self.tntPoolLen)
	pool := self.tntPool[shardNum]
	return pool[0]
}

func (self *ProxyConnection) getTnt16(shardingKey interface{}) *tarantool.Connection {
	if !self.schema.shardingEnabled {
		shardNum := uint32(self.listenNum)
		pool := self.tntPool[shardNum]

		tntIndex := self.getShardNumIndex(shardNum, len(pool))
		return pool[tntIndex]
		//return pool[rand.Intn(len(pool)-1)]
	}

	shardNum := getShardNum(shardingKey, self.tntPoolLen)
	pool := self.tntPool[shardNum]

	tntIndex := self.getShardNumIndex(shardNum, len(pool))
	return pool[tntIndex]
}

func (self *ProxyConnection) shardCounterFutureRunner() {
FOR_SHARD_COUNTER:
	for {
		select {
		case <-self.chanCntl:
			break FOR_SHARD_COUNTER
		case future := <-self.chanShardCounter:
			shardNum := future.shardNum
			future.shardIndex = self.shardCounter[shardNum]
			self.shardCounter[shardNum] = (self.shardCounter[shardNum] + 1) % future.lenPool
			close(future.ready)
		} //end select
	} //end for
}

func (self *ProxyConnection) packTnt16Error(body IprotoWriter, errMsg error) {
	PackUint32(body, (ErrorUnpackData<<8)|BadResponse15Status)
	errStr := errMsg.Error()
	body.WriteString(errStr)
}

func (self *ProxyConnection) packTnt16ResponseBody(body IprotoWriter, response *tarantool.Response, flags uint32) {
	if response.Code != 0 {
		PackUint32(body, (response.Code<<8)|BadResponse15Status)
		body.WriteString(response.Error)
		log.Printf("error response16 code=%d %s", response.Code, response.Error)
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

	tuple_buf := getBytesBufferFromPool()
	for _, tuple := range response.Data {
		tuple_buf.Reset()
		tupleIter, ok := tuple.([]interface{})
		if !ok {
			tupleIter = []interface{}{tuple}
		}
		for _, iValue := range tupleIter {
			switch val := iValue.(type) {
			case uint32:
				packUint64BER(tuple_buf, 4)
				PackUint32(tuple_buf, uint32(val))
			case uint64:
				packUint64BER(tuple_buf, 4)
				PackUint32(tuple_buf, uint32(val))
			case int64:
				packUint64BER(tuple_buf, 4)
				PackUint32(tuple_buf, uint32(val))
			case string:
				packUint64BER(tuple_buf, uint64(len(val)))
				tuple_buf.WriteString(val)
			default:
				// return nil, fmt.Errorf("error pack value: %v(%T)", val, val)
				log.Printf("error pack value: %v(%T)", val, val)
				tuple_buf.WriteString("")
			} //end switch*/
		} //end for

		PackUint32(body, uint32(tuple_buf.Len()))
		// add cardinality
		PackUint32(body, uint32(len(tupleIter)))

		io.Copy(body, tuple_buf)
	} //end for
	putBytesBufferToPool(tuple_buf)
	return
}

func (self *ProxyConnection) tarantool15SendResponse() {
	header := &bytes.Buffer{}
	var response *Response

FOR_CLIENT_POLL:
	for {
		select {
		case response = <-self.chanResponse:
		case <-self.chanCntl:
			break FOR_CLIENT_POLL
		default:
			runtime.Gosched()

			if len(self.chanResponse) == 0 {
				if err := self.writer.Flush(); err != nil {
					log.Println("client close connection")
					break FOR_CLIENT_POLL
				} //end if
			}

			select {
			case response = <-self.chanResponse:
			case <-self.chanCntl:
				break FOR_CLIENT_POLL
			} //end select
		} //end select

		header.Reset()

		PackUint32(header, response.RequestType)
		PackUint32(header, uint32(len(response.Body)))
		PackUint32(header, response.RequestId)

		if _, err := self.writer.Write(header.Bytes()); err != nil {
			log.Printf("error write response header: %s", err)
		}

		if _, err := self.writer.Write(response.Body); err != nil {
			log.Printf("error write response body: %s", err)
		}
		putBytesBufferToPool(bytes.NewBuffer(response.Body))
	} //end for

	// self.writer.Flush()
}

func (self *ProxyConnection) processIproto() {
	// bg response process for 15
	go self.tarantool15SendResponse()
	go self.shardCounterFutureRunner()

	// https://github.com/tarantool/tarantool/blob/stable/doc/box-protocol.txt
	mapCall := map[uint32]Tnt15Executor{
		RequestTypeSelect: self.executeRequestSelect,
		RequestTypeCall:   self.executeRequestCall,
		RequestTypeInsert: self.executeRequestInsert,
		RequestTypeDelete: self.executeRequestDelete,
		RequestTypeUpdate: self.executeRequestUpdate,
		RequestTypePing:   self.executeRequestPing,
	}

	// shared buffer for parse header
	iprotoHeader := bytes.NewBuffer(make([]byte, 4))
	for {
		var (
			requestType uint32
			bodyLength  uint32
			requestId   uint32
		)

		//read iproto header
		iprotoHeader.Reset()
		_, err := io.CopyN(iprotoHeader, self.reader, 12)
		if err != nil {
			if err != io.EOF {
				log.Printf("error read header15: - %s", err)
			}
			break
		}

		unpackUint32(iprotoHeader, &requestType)
		unpackUint32(iprotoHeader, &bodyLength)
		unpackUint32(iprotoHeader, &requestId)

		//read iproto body
		iprotoPackage := getBytesBufferFromPool()
		_, err = io.CopyN(iprotoPackage, self.reader, int64(bodyLength))
		if err != nil {
			if err != io.EOF {
				log.Printf("error read body15: - %s", err)
			}
			break
		}

		requestExecutor, ok := mapCall[requestType]
		if !ok {
			err := fmt.Errorf("unknown request type15: %d", requestType)
			body := getBytesBufferFromPool()
			self.packTnt16Error(body, err)

			response := &Response{
				RequestType: requestType,
				RequestId:   requestId,
				Body:        body.Bytes(),
			}
			self.chanResponse <- response
			continue
		}

		self.chanSem <- struct{}{}
		go func() {
			flags, tnt16Response, err := requestExecutor(requestType, requestId, iprotoPackage)
			putBytesBufferToPool(iprotoPackage)

			body := getBytesBufferFromPool()
			if err != nil {
				self.packTnt16Error(body, err)
			} else {
				self.packTnt16ResponseBody(body, tnt16Response, flags)
			}

			response := &Response{
				RequestType: requestType,
				RequestId:   requestId,
				Body:        body.Bytes(),
			}
			self.chanResponse <- response

			<-self.chanSem
		}()
	} //end for

	// close control chan for stop tarantool15SendResponse
	close(self.chanCntl)
}

func BytesToString(b []byte) string {
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh := reflect.StringHeader{bh.Data, bh.Len}
	return *(*string)(unsafe.Pointer(&sh))
}

func (self *ProxyConnection) unpackFieldByDefs(reader IprotoReader, requestType, fieldNo uint32, fieldType string) (val interface{}, err error) {
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
