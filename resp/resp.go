// Package resp provides RESP protocol serialization and deserialization
package resp

import (
	"fmt"
)

// CRLF is the line ending used in RESP
const CRLF = "\r\n"

// Type is the type of a RESP value
type Type byte

// Value types
const (
	typeNone         byte = 0
	TypeSimpleString Type = '+'
	TypeError        Type = '-'
	TypeInteger      Type = ':'
	TypeBulkString   Type = '$'
	TypeArray        Type = '*'
)

func (t Type) String() string {
	switch t {
	case TypeSimpleString:
		return "SimpleString"
	case TypeError:
		return "Error"
	case TypeInteger:
		return "Integer"
	case TypeBulkString:
		return "BulkString"
	case TypeArray:
		return "Array"
	default:
		return fmt.Sprintf("InvalidType %c", t)
	}
}

// ProtocolError is a RESP protocol error
type ProtocolError struct {
	Message string
}

func (e *ProtocolError) Error() string {
	return e.Message
}
func (e *ProtocolError) String() string {
	return e.Message
}

var (
	errInvalidStream  = &ProtocolError{"Invalid RESP stream"}
	errInvalidInteger = &ProtocolError{"Invalid integer"}
	errInvalidType    = &ProtocolError{"Invalid RESP type"}
	errInvalidSize    = &ProtocolError{Message: "Invalid size"}
)

// func AppendIntArray(buf []byte, values ...int64) []byte {
// 	buf = AppendArray(buf, int64(len(values)))
// 	for _, n := range values {
// 		buf = Integer(n).AppendRESP(buf)
// 	}
// 	return buf
// }

// func AppendCommand(buf []byte, cmd string, args ...string) []byte {
// 	buf = AppendArray(buf, int64(len(args)+1))
// 	bulk := BulkString{
// 		String: cmd,
// 		Valid:  true,
// 	}
// 	buf = bulk.AppendRESP(buf)
// 	for _, arg := range args {
// 		bulk.String = arg
// 		buf = bulk.AppendRESP(buf)
// 	}
// 	return buf
// }

// var (
// 	typSimpleString = reflect.TypeOf((*SimpleString)(nil)).Elem()
// 	typError        = reflect.TypeOf((*Error)(nil)).Elem()
// 	typInteger      = reflect.TypeOf((*Integer)(nil)).Elem()
// 	typBulkString   = reflect.TypeOf((*BulkString)(nil)).Elem()
// 	typArray        = reflect.TypeOf((*Array)(nil)).Elem()
// )

// func (t Type) Reflect() reflect.Type {
// 	switch t {
// 	case TypeSimpleString:
// 		return typSimpleString
// 	case TypeError:
// 		return typError
// 	case TypeInteger:
// 		return typInteger
// 	case TypeBulkString:
// 		return typBulkString
// 	case TypeArray:
// 		return typArray
// 	default:
// 		return nil
// 	}
// }
