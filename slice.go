package main

func fillSlice[T any](fn func() T, n int) []T {
	s := make([]T, 0, n)
	for i := 0; i < n; i++ {
		v := fn()
		s = append(s, v)
	}
	return s
}
