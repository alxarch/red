package resp

type Buffer struct {
	B []byte
}

func (b *Buffer) Array(arr ...Any) {
	b.B = Array(arr).AppendRESP(b.B)
}

func (b *Buffer) Int(n int64) {
	b.B = Integer(n).AppendRESP(b.B)
}

func (b *Buffer) Err(msg string) {
	b.B = Error(msg).AppendRESP(b.B)
}
func (b *Buffer) SimpleString(s string) {
	b.B = SimpleString(s).AppendRESP(b.B)
}
func (b *Buffer) BulkString(s string) {
	bulk := BulkString{String: s, Valid: true}
	b.B = bulk.AppendRESP(b.B)
}

func (b *Buffer) BulkStringArray(s ...string) {
	b.B = BulkStringArray(s).AppendRESP(b.B)
}

func (b *Buffer) BulkStringBytes(data []byte) {
	b.B = BulkStringBytes(data).AppendRESP(b.B)
}

func (b *Buffer) AppendRESP(buf []byte) []byte {
	return append(buf, b.B...)
}
