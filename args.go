package red

import (
	"math"
	"strconv"
	"time"

	"github.com/alxarch/red/resp"
)

// CommandBuilder builds a redis command
type CommandBuilder interface {
	BuildCommand(args *ArgBuilder) string
}

// CommandWriter writes a redis command
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

// Value returns the go value of an arg
func (a *Arg) Value() interface{} {
	switch a.typ {
	case argString:
		return a.str
	case argKey:
		return a.str
	case argInt:
		return int64(a.num)
	case argUint:
		return uint64(a.num)
	case argFloat64:
		return float64(math.Float64frombits(a.num))
	case argFloat32:
		return float32(math.Float64frombits(a.num))
	case argFalse:
		return false
	case argTrue:
		return true
	case argScore:
		return string(strconv.AppendFloat([]byte(a.str), math.Float64frombits(a.num), 'f', -1, 64))
	case argLex:
		return string(append([]byte{byte(a.num)}, a.str...))
	default:
		return nil
	}
}

// Equal checks if two args a are equal
func (a Arg) Equal(other Arg) bool {
	return a == other
}

// IsZero checks if an arg is the zero value
func (a Arg) IsZero() bool {
	return a == Arg{}
}

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

// Uint64 creates a uint64 argument.
func Uint64(n uint64) Arg {
	return Arg{typ: argUint, num: uint64(n)}
}

// Uint32 creates a uint32 argument.
func Uint32(n uint32) Arg {
	return Arg{typ: argUint, num: uint64(n)}
}

// Uint16 creates a uint16 argument.
func Uint16(n uint16) Arg {
	return Arg{typ: argUint, num: uint64(n)}
}

// Uint8 creates a uint8 argument.
func Uint8(n uint8) Arg {
	return Arg{typ: argUint, num: uint64(n)}
}

// Int creates an int argument.
func Int(n int) Arg {
	return Arg{typ: argInt, num: uint64(n)}
}

// Int64 creates an int64 argument.
func Int64(n int64) Arg {
	return Arg{typ: argInt, num: uint64(n)}
}

// Int32 creates an int32 argument.
func Int32(n int32) Arg {
	return Arg{typ: argInt, num: uint64(n)}
}

// Int16 creates an int16 argument.
func Int16(n int16) Arg {
	return Arg{typ: argInt, num: uint64(n)}
}

// Int8 creates an int8 argument.
func Int8(n int8) Arg {
	return Arg{typ: argInt, num: uint64(n)}
}

// Float64 creates a float argument.
func Float64(f float64) Arg {
	return Arg{typ: argFloat64, num: math.Float64bits(f)}
}

// Float32 creates a float argument.
func Float32(f float32) Arg {
	return Arg{typ: argFloat32, num: uint64(math.Float32bits(f))}
}

// Lex creates a lex range argument (ie '[foo', '(foo')
func Lex(lex string, inclusive bool) Arg {
	if inclusive {
		return Arg{typ: argLex, str: lex, num: 1}
	}
	return Arg{typ: argLex, str: lex, num: 0}
}

// Score creates an score range argument (ie '42.0', '(42.0')
func Score(f float64, inclusive bool) Arg {
	if inclusive {
		return Arg{typ: argScore, num: math.Float64bits(f)}

	}
	return Arg{typ: argScore, num: math.Float64bits(f), str: "("}

}

// Bool creates a boolean argument.
func Bool(b bool) Arg {
	if b {
		return Arg{typ: argTrue}
	}
	return Arg{typ: argFalse}
}

// Milliseconds creates an argument converting d to milliseconds
func Milliseconds(d time.Duration) Arg {
	return Arg{
		typ: argInt,
		num: uint64(d / time.Millisecond),
	}
}

// Seconds creates an argument converting d to seconds
func Seconds(d time.Duration) Arg {
	return Arg{
		typ: argInt,
		num: uint64(d / time.Second),
	}
}

// UnixSeconds creates an argument converting tm to unix timestamp
func UnixSeconds(tm time.Time) Arg {
	return Arg{
		typ: argInt,
		num: uint64(tm.Unix()),
	}
}

// UnixMilliseconds creates an argument converting tm to unix ms timestamp
func UnixMilliseconds(tm time.Time) Arg {
	ts := tm.UnixNano() / int64(time.Millisecond)
	return Arg{
		typ: argInt,
		num: uint64(ts),
	}
}

// MinScore creates a minus infinity score range argument
func MinScore() Arg {
	return String("-inf")
}

// MaxScore creates a max infinity score range argument
func MaxScore() Arg {
	return String("+inf")
}

// MaxLex creates a max infinity lex range arument
func MaxLex() Arg {
	return String("+")
}

// MinLex creates a minus infinity lex range arument
func MinLex() Arg {
	return String("-")
}

// ArgBuilder is an argument builder
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

// Writer writes commands to the underlying RESP writer
type Writer struct {
	dest    *resp.Writer
	scratch []byte // Reusable buffer used for numeric conversions on args
}

func (w *Writer) WriteCommand(keyPrefix, name string, args ...Arg) error {
	if err := w.dest.WriteArray(int64(len(args) + 1)); err != nil {
		return err
	}
	if err := w.dest.WriteBulkString(name); err != nil {
		return err
	}
	return w.WriteArgs(keyPrefix, args...)
}

func (w *Writer) Flush() error {
	return w.dest.Flush()
}
func (w *Writer) Reset(dest *resp.Writer) {
	w.dest = dest
}
func (w *Writer) WriteArgs(keyPrefix string, args ...Arg) (err error) {
	resp := w.dest
	for i := range args {
		switch arg := &args[i]; arg.typ {
		case argString:
			err = resp.WriteBulkString(arg.str)
		case argKey:
			err = resp.WriteBulkStringPrefix(keyPrefix, arg.str)
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