package red

// FlushDB writes a redis FLUSHDB command
func (b *batchAPI) FlushDB(async bool) *ReplyOK {
	b.args.Flag("ASYNC", async)
	return b.doSimpleStringOK("FLUSHDB", 0)
}
