package red

// Watch adds a WATCH command to the pipeline
func (c *Client) Watch(keys ...string) *ReplyOK {
	c.args.Keys(keys...)
	return c.doSimpleStringOK("WATCH", 0)
}

// Discard adds a DISCARD command to the pipeline
func (c *Client) Discard() *ReplyOK {
	return c.doSimpleStringOK("DISCARD", 0)
}

// Unwatch adds an UNWATCH command to the pipeline
func (c *Client) Unwatch() *ReplyOK {
	return c.doSimpleStringOK("UNWATCH", 0)
}

// Multi adds a MULTI/EXEC transaction to the pipeline
func (c *Client) Multi() *ReplyOK {
	return c.doSimpleStringOK("MULTI", 0)
}

// Exec commits a MULTI transaction
func (c *Client) Exec() *ReplyOK {
	return c.doSimpleStringOK("EXEC", 0)
}
