package main

import (
	"io"
)

// proxy constants
const (
	Version = "0.0.1"

	// proxy count connections for CPU util
	Tnt16PoolSize = 1

	// request type list for iproto 15
	RequestTypeInsert = 13
	RequestTypeSelect = 17
	RequestTypeUpdate = 19
	RequestTypeDelete = 21
	RequestTypeCall   = 22
	RequestTypePing   = 65280

	ErrorUnpackData = 10022

	BadResponse15Status = 2

	FlagReturnTuple = 1
	FlagAdd         = 2
	FlagReplace     = 4
	FlagPing        = 8
)

// UpdateOperationCode map code for RequestUpdate
var UpdateOperationCode = map[uint8]string{
	0: "=",
	1: "+",
	2: "&",
	3: "^",
	4: "|",
	5: ":",
}

// IprotoReader reader interface for iproto
type IprotoReader interface {
	io.ByteReader
	io.Reader
	Next(n int) []byte
}

// IprotoWriter writer interface for iproto
type IprotoWriter interface {
	io.Writer
	WriteString(s string) (n int, err error)
}

// ProxyConfigStruct yaml config struct
type ProxyConfigStruct struct {
	Listen15 []string   `yaml:"listen,flow"`
	Pass16   [][]string `yaml:"tarantool,flow"`
	Sharding bool       `yaml:"sharding_enabled,omitempty"`
	User     string     `yaml:"user,omitempty"`
	Password string     `yaml:"password,omitempty"`
	Space    []struct {
		ID     uint32   `yaml:"id"`
		Name   string   `yaml:"name"`
		Fields []string `yaml:"fields,flow"`
		Index  []struct {
			ID      uint32   `yaml:"id"`
			Name    string   `yaml:"name"`
			Columns []uint32 `yaml:"columns,flow"`
		}
	} `yaml:"space,flow"`
	Statsd struct {
		Server string `yaml:"server"`
		Prefix string `yaml:"prefix"`
	} `yaml:"statsd,omitempty"`
}
