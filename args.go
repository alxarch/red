package red

import (
	"math"
	"strconv"
	"time"

	"github.com/alxarch/red/resp"
)

type CommandBuilder interface {
	BuildCommand(args *ArgBuilder) string
}

type CommandWriter interface {
	WriteCommand(name string, args ...Arg) error
}

type argType uint

const (
	_ argType = iota
	argKey
	argString
	argInt
	argUint
	argFloat32
	argFloat64
	argTrue
	argFalse
	argScore
	argLex
)

// Arg is a command argument
type Arg struct {
	typ argType
	str string
	num uint64
}

func (arg *Arg) Value() interface{} {
	switch arg.typ {
	case argString:
		return arg.str
	case argKey:
		return arg.str
	case argInt:
		return int64(arg.num)
	case argUint:
		return uint64(arg.num)
	case argFloat64:
		return float64(math.Float64frombits(arg.num))
	case argFloat32:
		return float32(math.Float64frombits(arg.num))
	case argFalse:
		return false
	case argTrue:
		return true
	case argScore:
		return string(strconv.AppendFloat([]byte(arg.str), math.Float64frombits(arg.num), 'f', -1, 64))
	case argLex:
		return string(append([]byte{byte(arg.num)}, arg.str...))
	default:
		return nil
	}
}

func (a *Arg) Reset() {
	*a = Arg{}
}

func (a Arg) Equal(other Arg) bool {
	return a == other
}

func (a Arg) IsZero() bool {
	return a == Arg{}
}

// func (a *Arg) Type() ArgType {
// 	return a.typ
// }

// Key creates a string argument to be used as a key.
func Key(s string) Arg {
	return Arg{typ: argKey, str: s}
}

// String createa a string argument.
func String(s string) Arg {
	return Arg{typ: argString, str: s}
}

// Uint creates an unsigned int argument.
func Uint(n uint) Arg {
	return Arg{typ: argUint, num: uint64(n)}
}
func Uint64(n uint64) Arg {
	return Arg{typ: argUint, num: uint64(n)}
}
func Uint32(n uint32) Arg {
	return Arg{typ: argUint, num: uint64(n)}
}
func Uint16(n uint16) Arg {
	return Arg{typ: argUint, num: uint64(n)}
}
func Uint8(n uint8) Arg {
	return Arg{typ: argUint, num: uint64(n)}
}

// Int creates an int argument.
func Int(n int) Arg {
	return Arg{typ: argInt, num: uint64(n)}
}
func Int64(n int64) Arg {
	return Arg{typ: argInt, num: uint64(n)}
}
func Int32(n int32) Arg {
	return Arg{typ: argInt, num: uint64(n)}
}
func Int16(n int16) Arg {
	return Arg{typ: argInt, num: uint64(n)}
}
func Int8(n int8) Arg {
	return Arg{typ: argInt, num: uint64(n)}
}

// Float64 creates a float argument.
func Float64(f float64) Arg {
	return Arg{typ: argFloat64, num: math.Float64bits(f)}
}
func Lex(lex string, inclusive bool) Arg {
	if inclusive {
		return Arg{typ: argLex, str: lex, num: 1}
	}
	return Arg{typ: argLex, str: lex, num: 0}
}
func Score(f float64, inclusive bool) Arg {
	if inclusive {
		return Arg{typ: argScore, num: math.Float64bits(f)}

	}
	return Arg{typ: argScore, num: math.Float64bits(f), str: "("}

}

// Float32 creates a float argument.
func Float32(f float32) Arg {
	return Arg{typ: argFloat32, num: uint64(math.Float32bits(f))}
}

// Bool creates a boolean argument.
func Bool(b bool) Arg {
	if b {
		return Arg{typ: argTrue}
	}
	return Arg{typ: argFalse}
}

func Milliseconds(d time.Duration) Arg {
	return Arg{
		typ: argInt,
		num: uint64(d / time.Millisecond),
	}
}
func Seconds(d time.Duration) Arg {
	return Arg{
		typ: argInt,
		num: uint64(d / time.Second),
	}
}
func UnixSeconds(tm time.Time) Arg {
	return Arg{
		typ: argInt,
		num: uint64(tm.Unix()),
	}
}
func UnixMilliseconds(tm time.Time) Arg {
	ts := tm.UnixNano() / int64(time.Millisecond)
	return Arg{
		typ: argInt,
		num: uint64(ts),
	}
}

func MinInf() Arg {
	return String("-inf")
}
func MaxInf() Arg {
	return String("+inf")
}
func ScoreExclusive(score float64) Arg {
	return Score(score, false)
}
func ScoreInclusive(score float64) Arg {
	return Float64(score)
}
func LexExclusive(lex string) Arg {
	return Lex(lex, false)
}
func LexInclusive(lex string) Arg {
	return Lex(lex, true)
}
func LexMaxInf() Arg {
	return String("+")
}
func LexMinInf() Arg {
	return String("-")
}

type ArgBuilder struct {
	args []Arg
}

func (a *ArgBuilder) KV(key string, arg Arg) {
	a.args = append(a.args, Key(key), arg)
}
func (a *ArgBuilder) Key(key string) {
	a.args = append(a.args, Key(key))
}
func (a *ArgBuilder) Keys(keys ...string) {
	for _, arg := range keys {
		a.args = append(a.args, Key(arg))
	}
}

func (a *ArgBuilder) Field(name string, value Arg) {
	a.args = append(a.args, String(name), value)
}

func (a *ArgBuilder) String(str string) {
	a.args = append(a.args, String(str))
}
func (a *ArgBuilder) Int(n int64) {
	a.args = append(a.args, Int64(n))
}
func (a *ArgBuilder) Float(f float64) {
	a.args = append(a.args, Float64(f))
}

func (a *ArgBuilder) Score(score float64, include bool) {
	if include {
		a.Float(score)
	} else {
		a.Score(score, false)
	}
}

func (a *ArgBuilder) Option(option, value string) {
	if value != "" {
		a.args = append(a.args, String(option), String(value))
	}
}

func (a *ArgBuilder) Flag(flag string, ok bool) {
	if ok {
		a.String(flag)
	}
}
func (a *ArgBuilder) Strings(args ...string) {
	for _, arg := range args {
		a.args = append(a.args, String(arg))
	}
}
func (a *ArgBuilder) Unique(arg string, args ...string) {
	a.String(arg)
	if len(args) > 0 {
		head, tail := args[0], args[1:]
		if head != arg {
			a.String(head)
		}
		a.Strings(tail...)
	}
}
func (a *ArgBuilder) Arg(arg Arg) {
	a.args = append(a.args, arg)
}
func (a *ArgBuilder) Append(args ...Arg) {
	a.args = append(a.args, args...)
}

func (a *ArgBuilder) Len() int {
	return len(a.args)
}

func (b *ArgBuilder) Reset() {
	b.args = b.args[:0]
}

func (b *ArgBuilder) Clear() {
	args := b.args[:cap(b.args)]
	for i := range args {
		args[i] = Arg{}
	}
	b.args = args[:0]
}

func (b *ArgBuilder) Swap(args []Arg) []Arg {
	b.args, args = args, b.args
	return args
}
func (b *ArgBuilder) Args() (args []Arg) {
	b.args, args = args, b.args
	return
}

func QuickArgs(key string, args ...string) []Arg {
	out := make([]Arg, len(args)+1)
	str := out[1:]
	for i, arg := range args {
		str[i] = String(arg)
	}
	if key == "" {
		return str
	}
	out[0] = Key(key)
	return out
}

type Writer struct {
	KeyPrefix string
	resp      *resp.Writer
	scratch   []byte // Reusable buffer used for numeric conversions on args
}

func (w *Writer) WriteCommand(name string, args ...Arg) error {
	if err := w.resp.WriteArray(int64(len(args) + 1)); err != nil {
		return err
	}
	if err := w.resp.WriteBulkString(name); err != nil {
		return err
	}
	return w.WriteArgs(args...)
}

func (w *Writer) Flush() error {
	return w.resp.Flush()
}
func (w *Writer) Reset(dest *resp.Writer) {
	w.resp = dest
}
func (w *Writer) WriteArgs(args ...Arg) (err error) {
	resp := w.resp
	for i := range args {
		switch arg := &args[i]; arg.typ {
		case argString:
			err = resp.WriteBulkString(arg.str)
		case argKey:
			err = resp.WriteBulkStringPrefix(w.KeyPrefix, arg.str)
		case argInt:
			w.scratch = strconv.AppendInt(w.scratch[:0], int64(arg.num), 10)
			err = resp.WriteBulkStringBytes(w.scratch)
		case argUint:
			w.scratch = strconv.AppendUint(w.scratch[:0], arg.num, 10)
			err = resp.WriteBulkStringBytes(w.scratch)
		case argFloat32:
			w.scratch = strconv.AppendFloat(w.scratch[:0], math.Float64frombits(arg.num), 'f', -1, 32)
			err = resp.WriteBulkStringBytes(w.scratch)
		case argFloat64:
			w.scratch = strconv.AppendFloat(w.scratch[:0], math.Float64frombits(arg.num), 'f', -1, 64)
			err = resp.WriteBulkStringBytes(w.scratch)
		case argLex:
			w.scratch = append(w.scratch[:0], byte(arg.num))
			w.scratch = append(w.scratch, arg.str...)
			err = resp.WriteBulkStringBytes(w.scratch)
		case argScore:
			w.scratch = append(w.scratch[:0], arg.str...)
			w.scratch = strconv.AppendFloat(w.scratch, math.Float64frombits(arg.num), 'f', -1, 64)
			err = resp.WriteBulkStringBytes(w.scratch)
		case argFalse:
			err = resp.WriteBulkString("false")
		case argTrue:
			err = resp.WriteBulkString("true")
		}
		if err != nil {
			return
		}
	}
	return
}

// func Field(field string, value Arg) FieldArg {
// 	return FieldArg{Field: field, Value: value}
// }

// type Fields []FieldArg

// type FieldArg struct {
// 	Field string
// 	Value Arg
// }

// func KV(key string, value Arg) KVArg {
// 	return KVArg{Key: key, Value: value}
// }

// type KVs []KVArg

// type KVArg struct {
// 	Key   string
// 	Value Arg
// }
