package resp

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestReplyReadFrom(t *testing.T) {
	type Args struct {
		RESP   string
		Want   Any
		Target interface{}
	}
	reply := new(Message)
	b := bytes.NewBuffer(nil)
	r := bufio.NewReader(b)
	for i, args := range []Args{
		{"+OK\r\n", SimpleString("OK"), "OK"},
		{"-ERR\r\n", Error("ERR"), (error)(Error("ERR"))},
		{"-ERROR MESSAGE\r\n", Error("ERROR MESSAGE"), Error("ERROR MESSAGE")},
		{"+FOOBARBAZ\r\n", SimpleString("FOOBARBAZ"), "FOOBARBAZ"},
		{":42\r\n", Integer(42), int64(42)},
		{":-42\r\n", Integer(-42), int64(-42)},
		{":0\r\n", Integer(0), int64(0)},
		{"$3\r\nFOO\r\n", &BulkString{"FOO", true}, "FOO"},
		{"$10\r\nFOOBARBAZ!\r\n", &BulkString{"FOOBARBAZ!", true}, "FOOBARBAZ!"},
		{"$-1\r\n", &BulkString{}, nil},
		{"$0\r\n\r\n", &BulkString{"", true}, ""},
		{"*0\r\n", Array{}, make([]interface{}, 0)},
		{"*-1\r\n", (Array)(nil), ([]interface{})(nil)},
		{"*-1\r\n", (Array)(nil), ([]float64)(nil)},
		{"*3\r\n+OK\r\n+OK\r\n+OK\r\n", Array{SimpleString("OK"), SimpleString("OK"), SimpleString("OK")}, []interface{}{"OK", "OK", "OK"}},
		{"*2\r\n$2\r\n42\r\n*2\r\n$3\r\nFOO\r\n$3\r\nBAR\r\n", Array{
			&BulkString{String: "42", Valid: true},
			Array{
				&BulkString{String: "FOO", Valid: true},
				&BulkString{String: "BAR", Valid: true},
			},
		},
			[]interface{}{
				"42",
				[]interface{}{"FOO", "BAR"},
			},
		},
		{"*2\r\n$3\r\nFOO\r\n$3\r\nBAR\r\n", Array{
			&BulkString{String: "FOO", Valid: true},
			&BulkString{String: "BAR", Valid: true},
		},
			[]string{"FOO", "BAR"},
		},
		{"*2\r\n$3\r\nFOO\r\n$3\r\nBAR\r\n", Array{
			&BulkString{String: "FOO", Valid: true},
			&BulkString{String: "BAR", Valid: true},
		},
			[]interface{}{"FOO", "BAR"},
		},
		{"*2\r\n$3\r\nFOO\r\n$3\r\nBAR\r\n", Array{
			&BulkString{String: "FOO", Valid: true},
			&BulkString{String: "BAR", Valid: true},
		},
			[...]interface{}{"FOO", "BAR"},
		},
	} {
		b.Reset()
		b.WriteString(args.RESP)
		_, err := reply.ReadFrom(r)
		if err != nil {
			t.Errorf("%d %q Read failed %s", i, args.RESP, err)
		}
		v := reply.Any()
		if !reflect.DeepEqual(v, args.Want) {
			t.Errorf("%d %q Invalid value %v != %v", i, args.RESP, v, args.Want)
		}
		if out := v.AppendRESP(nil); string(out) != args.RESP {
			t.Errorf("%d %q Invalid RESP %q", i, args.RESP, out)
		}
		var target reflect.Value
		if args.Target == nil {
			var y interface{}
			target = reflect.ValueOf(&y)
		} else {
			target = reflect.New(reflect.TypeOf(args.Target))
		}
		x := target.Interface()

		if err := reply.Value().Decode(x); err != nil {
			t.Errorf("%d %q Decode failed %s", i, args.RESP, err)
		}
		actual := target.Elem().Interface()
		if !reflect.DeepEqual(actual, args.Target) {
			t.Errorf("%d %q Invalid value %v != %v", i, args.RESP, actual, args.Target)
		}

	}
}
func TestReplyReadFromN(t *testing.T) {
	rep := new(Message)
	b := new(Buffer)
	b.BulkString("foo")
	b.BulkString("bar")
	b.BulkString("baz")
	r := bufio.NewReader(bytes.NewReader(b.B))
	_, err := rep.ReadFrom(r)
	if err != nil {
		t.Errorf("Read failed %s", err)
	}
	v := rep.Value()
	var s string
	if err := v.Decode(&s); err != nil {
		t.Errorf("Invalid value %s", err)
	}
	if s != "foo" {
		t.Errorf("Invalid value %v", v)
	}

	expect := b.B
	actual := v.AppendRESP(nil)
	if string(expect) == string(actual) {
		t.Errorf("Invalid values %s", actual)
	}
}

func TestParseValue(t *testing.T) {
	b := new(Buffer)
	b.BulkStringArray("foo", "bar", "answer", "42")
	reply := Message{}
	_, err := reply.Parse(b.B)
	if err != nil {
		t.Errorf("Parse failed %s", err)
	}
	expect := b.B
	actual := reply.Any().AppendRESP(nil)
	if string(actual) != string(expect) {
		t.Errorf("Invalid reply  %q", actual)
	}

}
