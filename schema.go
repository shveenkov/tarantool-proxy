package main

import (
	"fmt"
	"io"
	"log"
	"strings"
)

// tarantool 1.5 schema types
const (
	SchemaTypeInt   = "int"
	SchemaTypeInt64 = "int64"
	SchemaTypeStr   = "str"
)

// FieldDefs iproto field defs for 1.5
type FieldDefs map[uint32]string

// SpaceInfo space info for 1.5
type SpaceInfo struct {
	name          string
	indexMap      FieldDefs
	typeFieldsMap FieldDefs
	typeIndexMap  map[uint32]FieldDefs
}

// Schema tarantool 1.5 schema
type Schema struct {
	shardingEnabled bool
	spaceInfoByID   map[uint32]*SpaceInfo
}

// NewSchema constuctor
func NewSchema(proxyConfig *ProxyConfigStruct) *Schema {
	SpaceInfoByID := make(map[uint32]*SpaceInfo)

	// init SpaceInfoByID from config
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
			info.indexMap[index.ID] = index.Name

			var indexTypeColumns = make(map[uint32]string)
			for columnNum, column := range index.Columns {
				fieldType, ok := info.typeFieldsMap[uint32(column)]
				if !ok {
					log.Fatalf("Error map index column type column_id=%d; index_name=%s", column, index.Name)
				}
				indexTypeColumns[uint32(columnNum)] = fieldType
			} //end for
			info.typeIndexMap[index.ID] = indexTypeColumns
		} //end for

		SpaceInfoByID[space.ID] = &info
	}

	return &Schema{
		shardingEnabled: proxyConfig.Sharding,
		spaceInfoByID:   SpaceInfoByID,
	}
}

// GetSpaceInfo get space info for 1.5 
func (p *Schema) GetSpaceInfo(spaceID15 uint32) (*SpaceInfo, error) {
	spaceInfo, ok := p.spaceInfoByID[spaceID15]
	if !ok {
		err := fmt.Errorf("space with id: %d does not exists in tarantool config", spaceID15)
		return nil, err
	}
	return spaceInfo, nil
}

// GetIndexName get index name from space
func (p *SpaceInfo) GetIndexName(indexID15 uint32) (string, error) {
	indexName, ok := p.indexMap[indexID15]
	if !ok {
		err := fmt.Errorf("index with id: %d, space_name: %s does not exists in tarantool config", indexID15, p.name)
		return "", err
	}
	return indexName, nil
}

// GetIndexDefs return index defs, name cardinality, etc
func (p *SpaceInfo) GetIndexDefs(indexID15 uint32) (FieldDefs, error) {
	indexDefs, ok := p.typeIndexMap[indexID15]
	if !ok {
		err := fmt.Errorf("index with id: %d, space_name: %s does not exists in tarantool config", indexID15, p.name)
		return nil, err
	}
	return indexDefs, nil
}

// PackUint32 pack Uint32
func PackUint32(w io.Writer, v uint32) error {
	_, err := w.Write([]byte{
		byte(v),
		byte(v >> 8),
		byte(v >> 16),
		byte(v >> 24),
	})
	return err
}

// PackUint64 pack Uint64
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
	return 0, fmt.Errorf("invalid ber-encoded integer: %s ", err.Error())
}

func unpackUint64Bytes(data []byte) (ret uint64) {
	ret = uint64(data[0])
	ret += uint64(data[1]) << 8
	ret += uint64(data[2]) << 16
	ret += uint64(data[3]) << 24
	ret += uint64(data[4]) << 32
	ret += uint64(data[5]) << 40
	ret += uint64(data[6]) << 48
	ret += uint64(data[7]) << 56
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
