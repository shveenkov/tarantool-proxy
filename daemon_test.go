package main

import (
	"bytes"
	_ "fmt"
	"gopkg.in/vmihailenco/msgpack.v2"
	"testing"
)

type myStruct struct {
	S string
	I int
	B bool
}

func TestMsgPack(t *testing.T) {
	// t.Errorf("foo test %d", 10)
	// msgpack.RegisterExt(1, myStruct{})

	s := myStruct{S: "Str", I: 10, B: false}
	b, err := msgpack.Marshal(&s)
	if err != nil {
		t.Errorf("marshaling error: %s", err)
	}

	var v interface{}
	err = msgpack.Unmarshal(b, &v)
	if err != nil {
		t.Errorf("unmarshaling error: %s", err)
	}
	// fmt.Printf("%[1]T : %#[1]v", v)
}

func a1() []int {
	a := make([]int, 10)
	a[1] = 2
	a[8] = 3
	a[7], a[0] = a[1], a[8]

	return a
}

func BenchmarkFoo(b *testing.B) {
	// b.N
	for i := 0; i < b.N; i++ {
		k := a1()
		_ = k
		//fmt.Sprintf("a:%d", a[1])
	}
}

var s string

func BenchmarkStringCreate(b *testing.B) {
	var array = []byte{'m', 'a', 'r', 'k', 'o', '1', 0}

	for i := 0; i < b.N; i++ {
		//s := string(array)
		buf := bytes.NewBuffer(array)
		s = BytesToString(buf.Next(5))
		buf.ReadByte()
	}
}

func BenchmarkStringCreateAlloc(b *testing.B) {
	var array = []byte{'m', 'a', 'r', 'k', 'o', '1', 0}

	for i := 0; i < b.N; i++ {
		//s := string(array)
		buf := bytes.NewBuffer(array)
		s = string(buf.Next(5))
		buf.ReadByte()
	}
}

func BenchmarkBufWrite(b *testing.B) {
	var array = []byte{'m', 'a', 'r', 'k', 'o', '1', 0}
	for i := 0; i < b.N; i++ {
		buf := &bytes.Buffer{}
		buf.Write(array)
		buf.Write(array)
		buf.Write(array)
		buf.Write(array)
		buf.Write(array)
		buf.Write(array)
		buf.Write(array)
		buf.Write(array)
		buf.Write(array)
		buf.Write(array)
	}
}

func BenchmarkBytesBufWrite(b *testing.B) {
	var array = []byte{'m', 'a', 'r', 'k', 'o', '1', 0}
	for i := 0; i < b.N; i++ {
		var buf []byte

		buf = append(buf, array...)
		buf = append(buf, array...)
		buf = append(buf, array...)
		buf = append(buf, array...)
		buf = append(buf, array...)
		buf = append(buf, array...)
		buf = append(buf, array...)
		buf = append(buf, array...)
		buf = append(buf, array...)
		buf = append(buf, array...)
	}
}

var bufr *bytes.Reader

func BenchmarkBufReader(b *testing.B) {
	var array = []byte{'m', 'a', 'r', 'k', 'o', '1', 0}
	for i := 0; i < b.N; i++ {
		bufr = bytes.NewReader(array)
	}
}

var bufw *bytes.Buffer

func BenchmarkBufWriter(b *testing.B) {
	var array = []byte{'m', 'a', 'r', 'k', 'o', '1', 0}
	for i := 0; i < b.N; i++ {
		bufw = bytes.NewBuffer(array)
	}
}
