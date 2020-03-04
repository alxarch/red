package resp_test

import (
	"reflect"
	"testing"

	"github.com/alxarch/red/resp"
)

func TestDecode(t *testing.T) {
	// row := sql.Row{}
	// row.Scan(nil)
	reply := resp.Message{}
	v, err := reply.Parse([]byte("*4\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbar\r\n$3\r\nbaz\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	var any []interface{}
	if err := v.Decode(&any); err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(any, []interface{}{"foo", "bar", "bar", "baz"}) {
		t.Errorf("Invalid decode %v", any)
	}
	var values []resp.Any
	if err := v.Decode(&values); err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(values, []resp.Any{
		&resp.BulkString{String: "foo", Valid: true},
		&resp.BulkString{String: "bar", Valid: true},
		&resp.BulkString{String: "bar", Valid: true},
		&resp.BulkString{String: "baz", Valid: true},
	}) {
		t.Errorf("Invalid values decode %v", values)
	}
	var m map[string]resp.Any
	if err := v.Decode(&m); err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(m, map[string]resp.Any{
		"foo": &resp.BulkString{String: "bar", Valid: true},
		"bar": &resp.BulkString{String: "baz", Valid: true},
	}) {
		t.Errorf("Invalid values map decode %v", m)
	}

}

func TestError_Decode(t *testing.T) {
	tests := []struct {
		name    string
		e       resp.Error
		arg     interface{}
		wantErr bool
	}{
		{"error", resp.Error("err"), (error)(resp.Error("err")), false},
		{"resp.Error", resp.Error("err"), resp.Error("err"), false},
		{"interface{}", resp.Error("err"), (interface{})((error)(resp.Error("err"))), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := reflect.New(reflect.TypeOf(tt.arg))
			x := target.Interface()
			if err := tt.e.Decode(x); (err != nil) != tt.wantErr {
				t.Errorf("Error.Decode() error = %v, wantErr %v", err, tt.wantErr)
			}
			actual := target.Elem().Interface()
			if !reflect.DeepEqual(actual, tt.arg) {
				t.Errorf("Error.Decode() invalid decode %v != %v", actual, tt.arg)

			}
		})
	}
}

func TestBulkString_Decode(t *testing.T) {
	tests := []struct {
		name    string
		s       resp.BulkString
		arg     interface{}
		wantErr bool
	}{
		{"nil", resp.BulkString{}, (interface{})(nil), false},
		{"null string", resp.BulkString{}, "", true},
		{"foo", resp.BulkString{Valid: true, String: "foo"}, "foo", false},
		{"foo any", resp.BulkString{Valid: true, String: "foo"}, (interface{})("foo"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var target reflect.Value
			if tt.arg == nil {
				var y interface{}
				target = reflect.ValueOf(&y)
			} else {
				target = reflect.New(reflect.TypeOf(tt.arg))
			}
			x := target.Interface()
			if err := tt.s.Decode(x); (err != nil) != tt.wantErr {
				t.Errorf("BulkString.Decode() error = %v, wantErr %v", err, tt.wantErr)
			}
			actual := target.Elem().Interface()
			if !reflect.DeepEqual(actual, tt.arg) {
				t.Errorf("BulkString.Decode() invalid decode %v != %v", actual, tt.arg)

			}
		})
	}
}
