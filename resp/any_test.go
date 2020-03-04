package resp_test

import (
	"reflect"
	"testing"

	"github.com/alxarch/red/resp"
)

func Test_Any(t *testing.T) {

	typArray := reflect.ValueOf((resp.Array)(nil)).Type()
	_ = typArray
}
