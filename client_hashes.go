package red

// Hashes

// HDel deletes fields from a map
func (c *Client) HDel(key, field string, fields ...string) *ReplyInteger {
	c.args.Key(key)
	c.args.Unique(field, fields...)
	return c.doInteger("HDEL")
}

// HExists checks if a field exists in a map
func (c *Client) HExists(key, field string) *ReplyBool {
	c.args.Key(key)
	c.args.String(field)
	return c.doBool("HEXISTS")
}

// HGet gets a map field value
func (c *Client) HGet(key, field string) *ReplyBulkString {
	c.args.Key(key)
	c.args.String(field)
	return c.doBulkString("HGET")
}

// HGetAll gets all field/value pairs in a map
func (c *Client) HGetAll(key string) *ReplyBulkStringArray {
	c.args.Key(key)
	return c.doBulkStringArray("HGETALL")

}

// HIncrBy increments a field in a map
func (c *Client) HIncrBy(key, field string, incr int64) *ReplyInteger {
	c.args.Key(key)
	c.args.String(field)
	c.args.Int(incr)
	return c.doInteger("HINCRBY")
}

// HIncrByFloat increments a field in a map by a float value
func (c *Client) HIncrByFloat(key, field string, incr float64) *ReplyFloat {
	c.args.Key(key)
	c.args.String(field)
	c.args.Float(incr)
	return c.doFloat("HINCRBYFLOAT")
}

// HKeys gets all field keys in a map
func (c *Client) HKeys(key string) *ReplyBulkStringArray {
	c.args.Key(key)
	return c.doBulkStringArray("HKEYS")
}

// HLen returns the number of fields in a map
func (c *Client) HLen(key string) *ReplyInteger {
	c.args.Key(key)
	return c.doInteger("HLEN")
}

// HMGet returns the values associated with the specified fields in the hash stored at key.
// For convenience in cases where the fields are already an array the first field is compared to `field`
func (c *Client) HMGet(key, field string, fields ...string) *ReplyBulkStringArray {
	c.args.Key(key)
	c.args.Unique(field, fields...)
	return c.doBulkStringArray("HMGET")
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

// HArg is a field-value pair
type HArg struct {
	Field string
	Value Arg
}

// H creates a field-value pair
func H(name string, value Arg) HArg {
	return HArg{Field: name, Value: value}
}

// HSet is a collection of field-value pairs
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
	return c.doInteger("HSET")
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
	return c.doInteger("HSET")
}

// HSetNXArg sets the value of a new field in a map using Arg
func (c *Client) HSetNXArg(key, field string, value Arg) *ReplyBool {
	c.args.Key(key)
	c.args.String(field)
	c.args.Arg(value)
	return c.doBool("HSETNX")
}

// HSetNX sets the value of a new field in a map
func (c *Client) HSetNX(key, field, value string) *ReplyBool {
	c.args.Key(key)
	c.args.String(field)
	c.args.String(value)
	return c.doBool("HSETNX")
}

// HStrLen returns the string length of the value of a field in a map
func (c *Client) HStrLen(key, field string) *ReplyInteger {
	c.args.Key(key)
	c.args.String(field)
	return c.doInteger("HSTRLEN")
}

// HVals returns the values of all fields in a map
func (c *Client) HVals(key string) *ReplyBulkStringArray {
	c.args.Key(key)
	return c.doBulkStringArray("HVALS")
}
