package red

import (
	"crypto/sha1"
	"encoding/hex"

	"github.com/alxarch/red/resp"
)

// Eval evaluates a Lua script
func (conn *Conn) Eval(dest interface{}, script string, numKeys int, args ...string) error {
	return conn.DoCommand(dest, evalCmd(script), evalArgs(script, numKeys, args...)...)
}
func evalCmd(script string) string {
	cmd := "EVAL"
	if isSHA1(script) {
		cmd = "EVALSHA"
	}
	return cmd
}

func isSHA1(str string) bool {
	if len(str) == 2*sha1.Size {
		var sha1 [2 * sha1.Size]byte
		_, err := hex.Decode(sha1[:], []byte(str))
		return err == nil
	}
	return false
}

// WriteEval is a convenience wrapper for WriteCommand
func (conn *Conn) WriteEval(script string, numKeys int, args ...string) error {
	return conn.WriteCommand(evalCmd(script), evalArgs(script, numKeys, args...)...)
}

// LoadScript loads a Lua script
func (conn *Conn) LoadScript(script string) (string, error) {
	sha1 := resp.BulkString{}
	if err := conn.DoCommand(&sha1, "SCRIPT", String("LOAD"), String(script)); err != nil {
		return "", err
	}
	if sha1.Null() {
		return "", resp.ErrNull
	}

	// Store loaded script for EVAL -> EVAL rewrites
	conn.scripts[String(script)] = sha1.String

	return sha1.String, nil
}

func evalArgs(script string, numKeys int, args ...string) []Arg {
	argv := make([]Arg, len(args)+2)
	argv[0] = String(script)
	argv[1] = Int(numKeys)
	for i, arg := range args {
		if i < numKeys {
			argv[i+2] = Key(arg)
		} else {
			argv[i+2] = String(arg)
		}
	}
	return argv
}
