package red

import (
	"math"
	"time"

	"github.com/alxarch/red/resp"
)

const MaxDBIndex = 16

const KeepTTL time.Duration = math.MinInt64

func DBIndexValid(index int) bool {
	return 0 <= index && index < MaxDBIndex
}

// StatusOK is the default success status
const StatusOK = resp.SimpleString("OK")

// StatusQueued is the status of a command in a MULTI/EXEC transaction
const StatusQueued = resp.SimpleString("QUEUED")

type noCopy struct{} //nolint:unused

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

// Mode defines command modes NX/XX
type Mode uint

// Execution modes
const (
	_ Mode = iota << 1
	NX
	XX
	CH
	INCR
	EX
	PX
	// MK
)

func (m Mode) XX() bool {
	return m&XX == XX
}
func (m Mode) NX() bool {
	return m&NX == NX
}
func (m Mode) INCR() bool {
	return m&INCR == INCR
}
func (m Mode) CH() bool {
	return m&CH == CH
}
func (m Mode) EX() bool {
	return m&EX == EX
}
func (m Mode) PX() bool {
	return m&PX == PX
}
func (m Mode) String() string {
	switch m {
	case NX:
		return "NX"
	case XX:
		return "XX"
	case CH:
		return "CH"
	case INCR:
		return "INCR"
	default:
		return ""

	}
}
