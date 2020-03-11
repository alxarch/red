package red

// Watch adds a WATCH command to the pipeline
func (b *Batch) Watch(keys ...string) *ReplyOK {
	b.args.Keys(keys...)
	return b.doSimpleStringOK("WATCH", 0)
}

// Unwatch adds an UNWATCH command to the pipeline
func (b *Batch) Unwatch() *ReplyOK {
	return b.doSimpleStringOK("UNWATCH", 0)
}
