package red

import (
	"math"
	"time"

	"github.com/alxarch/red/resp"
)

// MaxDBIndex is the max db index allowed by redis.
const MaxDBIndex = 16

// DBIndexValid checks if a DBIndex is valid
func DBIndexValid(index int) bool {
	return 0 <= index && index < MaxDBIndex
}

// KeepTTL sets the SET command's KEEPTTL flag in redis 6
const KeepTTL time.Duration = math.MinInt64

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

// XX checks if mode is XX
func (m Mode) XX() bool {
	return m&XX == XX
}

// NX checks if mode is NX
func (m Mode) NX() bool {
	return m&NX == NX
}

// INCR checks if mode is INCR
func (m Mode) INCR() bool {
	return m&INCR == INCR
}

// CH checks if mode is CH
func (m Mode) CH() bool {
	return m&CH == CH
}

// EX checks if mode is EX
func (m Mode) EX() bool {
	return m&EX == EX
}

// PX checks if mode is PX
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
