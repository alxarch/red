package resp

import (
	"bytes"
	"testing"
)

func Test_Writer(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	w := NewWriter(buffer)
	w.WriteArray(2)
	w.WriteBulkString("SELECT")
	err := w.WriteBulkString("16")
	if err != nil {
		t.Errorf("WriteCommand failed %s", err)

	}
	if buffer.String() != "" {
		t.Errorf("Premature flush")
	}
	if err := w.Flush(); err != nil {
		t.Errorf("Flush failed %s", err)
	}
	if buffer.String() != "*2\r\n$6\r\nSELECT\r\n$2\r\n16\r\n" {
		t.Errorf("Invalid flush %s", buffer.String())
	}
}
