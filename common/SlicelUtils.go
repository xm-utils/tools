package common

// Contains 判断slice中是否包含某个元素
func Contains[T comparable](s []T, i T) bool {
	for _, a := range s {
		if a == i {
			return true
		}
	}
	return false
}

// Deduplicate 去重
func Deduplicate[T comparable](list []T) []T {
	seen := make(map[T]bool)
	var filtered []T
	for _, val := range list {
		if _, ok := seen[val]; !ok {
			seen[val] = true
			filtered = append(filtered, val)
		}
	}
	return filtered
}

// Union 求并集
func Union[T comparable](slice1, slice2 []T) []T {
	m := make(map[T]int)
	for _, v := range slice1 {
		m[v]++
	}
	for _, v := range slice2 {
		times, _ := m[v]
		if times == 0 {
			slice1 = append(slice1, v)
		}
	}
	return slice1
}

// Intersect 求交集
func Intersect[T comparable](slice1, slice2 []T) []T {
	m := make(map[T]int)
	nn := make([]T, 0)
	for _, v := range slice1 {
		m[v]++
	}
	for _, v := range slice2 {
		times, _ := m[v]
		if times == 1 {
			nn = append(nn, v)
		}
	}
	return nn
}

// Difference 求差集 slice1-并集
func Difference[T comparable](slice1, slice2 []T) []T {
	m := make(map[T]int)
	nn := make([]T, 0)
	inter := Intersect(slice1, slice2)
	for _, v := range inter {
		m[v]++
	}
	for _, value := range slice1 {
		times, _ := m[value]
		if times == 0 {
			nn = append(nn, value)
		}
	}
	return nn
}

// ListToMap 将切片转换为Map，需提供keyFunc和valueFunc
func ListToMap[T any, K comparable, V any](list []T, keyFunc func(T) K, valueFunc func(T) V) map[K][]V {
	result := make(map[K][]V, len(list))
	for _, item := range list {
		key := keyFunc(item)
		if _, ok := result[key]; !ok {
			result[key] = make([]V, 0)
		}
		v := valueFunc(item)
		result[key] = append(result[key], v)
	}
	return result
}
