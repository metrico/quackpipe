package data_types

func FastFillArray[T any](arr []T, data T) []T {
	if len(arr) == 0 {
		return arr
	}
	arr[0] = data
	for i := 1; i < len(arr); i *= 2 {
		copy(arr[i:], arr[:i])
	}
	return arr
}
