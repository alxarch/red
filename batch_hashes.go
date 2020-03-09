package red

// Hashes

// HDel deletes fields from a map
func (b *batchAPI) HDel(key, field string, fields ...string) *ReplyInteger {
	b.args.Key(key)
	b.args.Unique(field, fields...)
	return b.doInteger("HDEL")
}

// HExists checks if a field exists in a map
func (b *batchAPI) HExists(key, field string) *ReplyBool {
	b.args.Key(key)
	b.args.String(field)
	return b.doBool("HEXISTS")
}

// HGet gets a map field value
func (b *batchAPI) HGet(key, field string) *ReplyBulkString {
	b.args.Key(key)
	b.args.String(field)
	return b.doBulkString("HGET")
}

// HGetAll gets all field/value pairs in a map
func (b *batchAPI) HGetAll(key string) *ReplyBulkStringArray {
	b.args.Key(key)
	return b.doBulkStringArray("HGETALL")

}

// HIncrBy increments a field in a map
func (b *batchAPI) HIncrBy(key, field string, incr int64) *ReplyInteger {
	b.args.Key(key)
	b.args.String(field)
	b.args.Int(incr)
	return b.doInteger("HINCRBY")
}

// HIncrByFloat increments a field in a map by a float value
func (b *batchAPI) HIncrByFloat(key, field string, incr float64) *ReplyFloat {
	b.args.Key(key)
	b.args.String(field)
	b.args.Float(incr)
	return b.doFloat("HINCRBYFLOAT")
}

// HKeys gets all field keys in a map
func (b *batchAPI) HKeys(key string) *ReplyBulkStringArray {
	b.args.Key(key)
	return b.doBulkStringArray("HKEYS")
}

// HLen returns the number of fields in a map
func (b *batchAPI) HLen(key string) *ReplyInteger {
	b.args.Key(key)
	return b.doInteger("HLEN")
}

// HMGet returns the values associated with the specified fields in the hash stored at key.
// For convenience in cases where the fields are already an array the first field is compared to `field`
func (b *batchAPI) HMGet(key, field string, fields ...string) *ReplyBulkStringArray {
	b.args.Key(key)
	b.args.Unique(field, fields...)
	return b.doBulkStringArray("HMGET")
}

// HMSet sets the values of multiple fields in a map
// As per Redis 4.0.0, HMSET is considered deprecated. Please use HSET in new code.
// func (b *Batch) HMSet(key string, fields ...string) *ReplyOK {
// 	reply := ReplyOK{}
// 	args := &c.args
// 	args.Key(key)
// 	args.Strings(fields...)

// 	b.do("HMSET", &reply)
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
func (b *batchAPI) HSet(key, field, value string, entries ...string) *ReplyInteger {
	b.args.Key(key)
	b.args.String(field)
	b.args.String(value)
	var k, v string
	for len(entries) >= 2 {
		k, v, entries = entries[0], entries[1], entries[2:]
		b.args.Strings(k, v)
	}
	return b.doInteger("HSET")
}

// HSetArg adds an HSet command to the pipeline using Arg
func (b *batchAPI) HSetArg(key string, field string, value Arg, entries ...HArg) *ReplyInteger {
	b.args.Key(key)
	b.args.String(field)
	b.args.Arg(value)

	for i := range entries {
		f := &entries[i]
		b.args.String(f.Field)
		b.args.Arg(f.Value)
	}
	return b.doInteger("HSET")
}

// HSetNXArg sets the value of a new field in a map using Arg
func (b *batchAPI) HSetNXArg(key, field string, value Arg) *ReplyBool {
	b.args.Key(key)
	b.args.String(field)
	b.args.Arg(value)
	return b.doBool("HSETNX")
}

// HSetNX sets the value of a new field in a map
func (b *batchAPI) HSetNX(key, field, value string) *ReplyBool {
	b.args.Key(key)
	b.args.String(field)
	b.args.String(value)
	return b.doBool("HSETNX")
}

// HStrLen returns the string length of the value of a field in a map
func (b *batchAPI) HStrLen(key, field string) *ReplyInteger {
	b.args.Key(key)
	b.args.String(field)
	return b.doInteger("HSTRLEN")
}

// HVals returns the values of all fields in a map
func (b *batchAPI) HVals(key string) *ReplyBulkStringArray {
	b.args.Key(key)
	return b.doBulkStringArray("HVALS")
}
