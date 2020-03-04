package red

func (c *Client) FlushDB(async bool) *ReplyOK {
	c.args.Flag("ASYNC", async)
	reply := ReplyOK{}
	c.do("FLUSHDB", &reply)
	return &reply
}
