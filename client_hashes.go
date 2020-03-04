package red

// Hashes

// HDel deletes fields from a map
func (c *Client) HDel(key, field string, fields ...string) *ReplyInteger {
	reply := ReplyInteger{}
	c.args.Key(key)
	c.args.Unique(field, fields...)
	c.do("HDEL", &reply)
	return &reply

}

// HExists checks if a field exists in a map
func (c *Client) HExists(key, field string) *ReplyBool {
	reply := ReplyBool{}
	args := &c.args
	args.Key(key)
	args.String(field)

	c.do("HEXISTS", &reply)
	return &reply

}

// HGet gets a map field value
func (c *Client) HGet(key, field string) *ReplyBulkString {
	reply := ReplyBulkString{}
	args := &c.args
	args.Key(key)
	args.String(field)

	c.do("HGET", &reply)
	return &reply

}

// HGetAll gets all field/value pairs in a map
func (c *Client) HGetAll(key string) *ReplyBulkStringArray {
	reply := ReplyBulkStringArray{}
	args := &c.args
	args.Key(key)
	c.do("HGETALL", &reply)
	return &reply

}

// Command implements Commander interface
func (c *Client) HIncrBy(key, field string, incr int64) *ReplyInteger {
	reply := ReplyInteger{}
	args := &c.args
	args.Key(key)
	args.String(field)
	args.Int(incr)
	c.do("HINCRBY", &reply)
	return &reply

}

// Command implements Commander interface
func (c *Client) HIncrByFloat(key, field string, incr float64) *ReplyFloat {
	reply := ReplyFloat{}
	args := &c.args
	args.Key(key)
	args.String(field)
	args.Float(incr)

	c.do("HINCRBYFLOAT", &reply)
	return &reply

}

// Command implements Commander interface
func (c *Client) HKeys(key string) *ReplyBulkStringArray {
	reply := ReplyBulkStringArray{}
	args := &c.args
	args.Key(key)
	c.do("HKEYS", &reply)
	return &reply

}

// Command implements Commander interface
func (c *Client) HLen(key string) *ReplyInteger {
	reply := ReplyInteger{}
	args := &c.args
	args.Key(key)

	c.do("HLEN", &reply)
	return &reply

}

// HMGet returns the values associated with the specified fields in the hash stored at key.
// For convenience in cases where the fields are already an array the first field is compared to `field`
func (c *Client) HMGet(key, field string, fields ...string) *ReplyBulkStringArray {
	reply := ReplyBulkStringArray{}
	c.args.Key(key)
	c.args.Unique(field, fields...)
	c.do("HMGET", &reply)
	return &reply

}

// HMSet sets the values of multiple fields in a map
// As per Redis 4.0.0, HMSET is considered deprecated. Please use HSET in new code.
// func (c *Client) HMSet(key string, fields ...string) *ReplyOK {
// 	reply := ReplyOK{}
// 	args := &c.args
// 	args.Key(key)
// 	args.Strings(fields...)

// 	c.do("HMSET", &reply)
// 	return &reply

// }

type HArg struct {
	Field string
	Value Arg
}

// func Field(name string, value Arg) HArg {
// 	return HArg{Field: name, Value: value}
// }

type HSet []HArg

// HSet adds an HSet command to the pipeline
func (c *Client) HSet(key, field, value string, entries ...string) *ReplyInteger {
	c.args.Key(key)
	c.args.String(field)
	c.args.String(value)
	var k, v string
	for len(entries) >= 2 {
		k, v, entries = entries[0], entries[1], entries[2:]
		c.args.Strings(k, v)
	}
	reply := ReplyInteger{}
	c.do("HSET", &reply)
	return &reply
}

// HSetArg adds an HSet command to the pipeline using Arg
func (c *Client) HSetArg(key string, field string, value Arg, entries ...HArg) *ReplyInteger {
	c.args.Key(key)
	c.args.String(field)
	c.args.Arg(value)

	for i := range entries {
		f := &entries[i]
		c.args.String(f.Field)
		c.args.Arg(f.Value)
	}
	reply := ReplyInteger{}
	c.do("HSET", &reply)
	return &reply
}

// HSetNXArg sets the value of a new field in a map using Arg
func (c *Client) HSetNXArg(key, field string, value Arg) *ReplyBool {
	reply := ReplyBool{}
	args := &c.args
	args.Key(key)
	args.String(field)
	args.Arg(value)

	c.do("HSETNX", &reply)
	return &reply

}

// HSetNX sets the value of a new field in a map
func (c *Client) HSetNX(key, field, value string) *ReplyBool {
	reply := ReplyBool{}
	args := &c.args
	args.Key(key)
	args.String(field)
	args.String(value)

	c.do("HSETNX", &reply)
	return &reply

}

// HStrLen returns the string length of the value of a field in a map
func (c *Client) HStrLen(key, field string) *ReplyInteger {
	reply := ReplyInteger{}
	args := &c.args
	args.Key(key)
	args.String(field)

	c.do("HSTRLEN", &reply)
	return &reply

}

// HVals returns the values of all fields in a map
func (c *Client) HVals(key string) *ReplyBulkStringArray {
	reply := ReplyBulkStringArray{}
	args := &c.args
	args.Key(key)
	c.do("HVALS", &reply)
	return &reply

}
