package red

// Watch adds a WATCH command to the pipeline
func (c *Client) Watch(keys ...string) *ReplyOK {
	reply := ReplyOK{}
	c.args.Keys(keys...)
	c.do("WATCH", &reply)
	return &reply
}

// Discard adds a DISCARD command to the pipeline
func (c *Client) Discard() *ReplyOK {
	reply := ReplyOK{}
	c.do("DISCARD", &reply)
	return &reply
}

// Unwatch adds an UNWATCH command to the pipeline
func (c *Client) Unwatch() *ReplyOK {
	reply := ReplyOK{}
	c.do("UNWATCH", &reply)
	return &reply
}

// Multi adds a MULTI/EXEC transaction to the pipeline
func (c *Client) Multi() *ReplyOK {
	reply := ReplyOK{}
	c.do("MULTI", &reply)
	return &reply
}

// Exec commits a MULTI transaction
func (c *Client) Exec() *ReplyOK {
	reply := ReplyOK{}
	c.do("EXEC", &reply)
	return &reply
}
