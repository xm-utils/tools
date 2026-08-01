package common

import "golang.org/x/crypto/bcrypt"

// HashPassword 对明文密码进行加盐哈希
func HashPassword(password string) (string, error) {
	// 1. 将密码转换为字节切片
	bytes := []byte(password)

	// 2. 使用 DefaultCost (通常为10) 生成哈希
	// bcrypt 会自动生成随机盐值并嵌入结果中
	hashedBytes, err := bcrypt.GenerateFromPassword(bytes, bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}

	// 3. 返回哈希字符串，格式如: $2a$10$N9qo8uLOickgx2ZMRZoMye...
	return string(hashedBytes), nil
}

// CheckPassword 校验明文密码与哈希是否匹配
func CheckPassword(password, hashedPassword string) error {
	// 1. 将明文密码和存储的哈希转换为字节切片
	passwordBytes := []byte(password)
	hashedBytes := []byte(hashedPassword)

	// 2. 自动从 hashedBytes 中提取盐值进行比对
	// 若匹配返回 nil，不匹配返回 ErrMismatchedHashAndPassword
	return bcrypt.CompareHashAndPassword(hashedBytes, passwordBytes)
}
