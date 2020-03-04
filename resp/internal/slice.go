package internal

func MakeSliceInterface(arr []interface{}, size int) []interface{} {
	if arr == nil {
		return make([]interface{}, size)
	}
	if size <= len(arr) {
		var drop []interface{}
		arr, drop = arr[:size], arr[size:]
		for i := range drop {
			drop[i] = nil
		}
		return arr
	}
	if size <= cap(arr) {
		return arr[:size]
	}
	return make([]interface{}, size)
}

func MakeSliceString(arr []string, size uint32) []string {
	if arr == nil {
		return make([]string, size)
	}
	if size <= uint32(len(arr)) {
		var drop []string
		arr, drop = arr[:size], arr[size:]
		for i := range drop {
			drop[i] = ""
		}
		return arr
	}
	if size <= uint32(cap(arr)) {
		return arr[:size]
	}
	return make([]string, size)
}
