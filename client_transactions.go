package red

// Watch adds a WATCH command to the pipeline
func (b *batchAPI) Watch(keys ...string) *ReplyOK {
	b.args.Keys(keys...)
	return b.doSimpleStringOK("WATCH", 0)
}

// Discard adds a DISCARD command to the pipeline
func (b *batchAPI) Discard() *ReplyOK {
	return b.doSimpleStringOK("DISCARD", 0)
}

// Unwatch adds an UNWATCH command to the pipeline
func (b *batchAPI) Unwatch() *ReplyOK {
	return b.doSimpleStringOK("UNWATCH", 0)
}

// Multi adds a MULTI/EXEC transaction to the pipeline
func (b *batchAPI) Multi() *ReplyOK {
	return b.doSimpleStringOK("MULTI", 0)
}

// Exec commits a MULTI transaction
func (b *batchAPI) Exec() *ReplyAny {
	return b.doAny("EXEC")
}
