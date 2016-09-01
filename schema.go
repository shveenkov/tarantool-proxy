package main

import (
	"fmt"
	"io"
	"log"
	"strings"
)

const (
	SchemaTypeInt   = "int"
	SchemaTypeInt64 = "int64"
	SchemaTypeStr   = "str"
)

type FieldDefs map[uint32]string

type SpaceInfo struct {
	name          string
	indexMap      FieldDefs
	typeFieldsMap FieldDefs
	typeIndexMap  map[uint32]FieldDefs
}

type Schema struct {
	shardingEnabled bool
	spaceInfoById   map[uint32]*SpaceInfo
}

func NewSchema(proxyConfig *ProxyConfigStruct) *Schema {
	SpaceInfoById := make(map[uint32]*SpaceInfo)

	// init SpaceInfoById from config
	for _, space := range proxyConfig.Space {
		info := SpaceInfo{
			name:          space.Name,
			indexMap:      make(map[uint32]string),
			typeFieldsMap: make(map[uint32]string),
			typeIndexMap:  make(map[uint32]FieldDefs),
		}

		// fields type info for request pack
		for fieldNo, fieldDef := range space.Fields {
			fieldInfo := strings.SplitN(fieldDef, ":", 2)
			fieldType := SchemaTypeStr
			if len(fieldInfo) == 2 {
				fieldType = fieldInfo[1]
			}
			info.typeFieldsMap[uint32(fieldNo)] = fieldType
		}

		// mapping index_id => index_name
		// mapping index_id => [column1_type, column2_type, ...]
		for _, index := range space.Index {
			info.indexMap[index.Id] = index.Name

			var indexTypeColumns = make(map[uint32]string)
			for columnNum, column := range index.Columns {
				fieldType, ok := info.typeFieldsMap[uint32(column)]
				if !ok {
					log.Fatalf("Error map index column type column_id=%d; index_name=%s", column, index.Name)
				}
				indexTypeColumns[uint32(columnNum)] = fieldType
			} //end for
			info.typeIndexMap[index.Id] = indexTypeColumns
		} //end for

		SpaceInfoById[space.Id] = &info
	}

	return &Schema{
		shardingEnabled: proxyConfig.Sharding,
		spaceInfoById:   SpaceInfoById,
	}
}

func (self *Schema) GetSpaceInfo(spaceId15 uint32) (*SpaceInfo, error) {
	spaceInfo, ok := self.spaceInfoById[spaceId15]
	if !ok {
		err := fmt.Errorf("space with id: %d does not exists in tarantool config", spaceId15)
		return nil, err
	}
	return spaceInfo, nil
}

func (self *SpaceInfo) GetIndexName(indexId15 uint32) (string, error) {
	indexName, ok := self.indexMap[indexId15]
	if !ok {
		err := fmt.Errorf("index with id: %d, space_name: %s does not exists in tarantool config", indexId15, self.name)
		return "", err
	}
	return indexName, nil
}

func (self *SpaceInfo) GetIndexDefs(indexId15 uint32) (FieldDefs, error) {
	indexDefs, ok := self.typeIndexMap[indexId15]
	if !ok {
		err := fmt.Errorf("index with id: %d, space_name: %s does not exists in tarantool config", indexId15, self.name)
		return nil, err
	}
	return indexDefs, nil
}

func PackUint32(w io.Writer, v uint32) error {
	_, err := w.Write([]byte{
		byte(v),
		byte(v >> 8),
		byte(v >> 16),
		byte(v >> 24),
	})
	return err
}

func PackUint64(w io.Writer, v uint64) error {
	_, err := w.Write([]byte{
		byte(v),
		byte(v >> 8),
		byte(v >> 16),
		byte(v >> 24),
		byte(v >> 32),
		byte(v >> 40),
		byte(v >> 48),
		byte(v >> 56),
	})
	return err
}

func packUint64BER(w io.Writer, v uint64) error {
	const maxLen = 10 // 10 bytes is sufficient to hold a 128-base 64-bit uint.

	if v == 0 {
		w.Write([]byte{0})
		return nil
	}

	buf := make([]byte, maxLen)
	n := maxLen - 1

	for ; n >= 0 && v > 0; n-- {
		buf[n] = byte(v & 0x7f)
		v >>= 7

		if n != (maxLen - 1) {
			buf[n] |= 0x80
		}
	}

	_, err := w.Write(buf[n+1:])
	return err
}

func unpackUint64BER(r io.ByteReader, valueBits int) (v uint64, err error) {
	v = 0
	for i := 0; i <= valueBits/7; i++ {
		var b byte
		b, err = r.ReadByte()
		if err != nil {
			break
		}
		v <<= 7
		v |= uint64(b & 0x7f)

		if b&0x80 == 0 {
			return
		}
	}
	return 0, fmt.Errorf("invalid ber-encoded integer: %s!", err.Error())
}

func unpackUint64Bytes(data []byte) (ret uint32) {
	ret = uint32(data[0])
	ret += uint32(data[1]) << 8
	ret += uint32(data[2]) << 16
	ret += uint32(data[3]) << 24
	ret += uint32(data[4]) << 32
	ret += uint32(data[5]) << 40
	ret += uint32(data[6]) << 48
	ret += uint32(data[7]) << 56
	return
}

func unpackUint32Bytes(data []byte) (ret uint32) {
	ret = uint32(data[0])
	ret += uint32(data[1]) << 8
	ret += uint32(data[2]) << 16
	ret += uint32(data[3]) << 24
	return
}

func unpackUint32(r io.Reader, value *uint32) (err error) {
	data := make([]byte, 4)
	if _, err = r.Read(data); err != nil {
		return
	}

	// *value = unpackUint32Bytes(data)
	*value = uint32(data[0])
	*value += uint32(data[1]) << 8
	*value += uint32(data[2]) << 16
	*value += uint32(data[3]) << 24
	return
}

func unpackUint8(r io.Reader, value *uint8) (err error) {
	data := make([]byte, 1)
	if _, err = r.Read(data); err != nil {
		return
	}

	*value = uint8(data[0])
	return
}
