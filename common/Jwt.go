package common

import (
	"cashloan/internal/constant"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/XingMenTech/common/utils"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v4"
)

const (
	TokenSecret      = "game" //加密密钥
	TokenInvalidTime = 24     //小时 - 访问令牌有效期
	RefreshTokenTime = 7 * 24 //小时 - 刷新令牌有效期
)

type Claims struct {
	Uid      int64  `json:"uid"`
	Username string `json:"username"`
	UserType int    `json:"user_type"` // 用户类型
	DeviceId string `json:"device_id"` // 设备ID
	ClientIp string `json:"client_ip"` // 客户端IP
	jwt.StandardClaims
}

// RefreshClaims 刷新令牌的声明
type RefreshClaims struct {
	Uid         int64  `json:"uid"`
	DeviceId    string `json:"device_id"`    // 设备ID
	OriginalJti string `json:"original_jti"` // 原始访问令牌的JTI
	jwt.StandardClaims
}

var jwtSecret = []byte(TokenSecret)

// GenerateAccessToken 生成访问令牌
func GenerateAccessToken(uid int64, username string, userType int, deviceId string, clientIp string) (string, error) {
	now := time.Now()
	expireTime := now.Add(time.Duration(TokenInvalidTime) * time.Hour)
	jti := utils.RandomString(32) // JWT ID，用于防止重放攻击

	claims := Claims{
		Uid:      uid,
		Username: username,
		UserType: userType,
		DeviceId: deviceId,
		ClientIp: clientIp,
		StandardClaims: jwt.StandardClaims{
			ExpiresAt: expireTime.Unix(),
			IssuedAt:  now.Unix(),
			NotBefore: now.Unix(),
			Issuer:    "common-jwt-service",
			Subject:   fmt.Sprintf("user_%d", uid),
			Id:        jti,
		},
	}

	tokenClaims := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token, err := tokenClaims.SignedString(jwtSecret)
	if err != nil {
		return "", err
	}

	return token, nil
}

// GenerateRefreshToken 生成刷新令牌
func GenerateRefreshToken(uid int64, originalJti string, deviceId string) (string, error) {
	now := time.Now()
	expireTime := now.Add(time.Duration(RefreshTokenTime) * time.Hour)

	claims := RefreshClaims{
		Uid:         uid,
		DeviceId:    deviceId,
		OriginalJti: originalJti,
		StandardClaims: jwt.StandardClaims{
			ExpiresAt: expireTime.Unix(),
			IssuedAt:  now.Unix(),
			NotBefore: now.Unix(),
			Issuer:    "common-jwt-service",
			Subject:   fmt.Sprintf("refresh_user_%d", uid),
		},
	}

	tokenClaims := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token, err := tokenClaims.SignedString(jwtSecret)
	if err != nil {
		return "", err
	}

	return token, nil
}

// GenerateTokenPair 生成访问令牌和刷新令牌对
func GenerateTokenPair(uid int64, username string, userType int, deviceId string, clientIp string) (accessToken string, refreshToken string, err error) {
	accessToken, err = GenerateAccessToken(uid, username, userType, deviceId, clientIp)
	if err != nil {
		return "", "", err
	}

	// 解析访问令牌获取JTI
	claims, err := ParseAccessToken(accessToken)
	if err != nil {
		return "", "", err
	}

	refreshToken, err = GenerateRefreshToken(uid, claims.Id, deviceId)
	if err != nil {
		return "", "", err
	}

	return accessToken, refreshToken, nil
}

// ParseAccessToken 解析访问令牌
func ParseAccessToken(token string) (*Claims, error) {
	tokenClaims, err := jwt.ParseWithClaims(token, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		// 验证签名方法
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return jwtSecret, nil
	})

	if err != nil {
		return nil, err
	}

	claims, ok := tokenClaims.Claims.(*Claims)
	if !ok || !tokenClaims.Valid {
		return nil, errors.New("invalid token")
	}

	return claims, nil
}

// ParseRefreshToken 解析刷新令牌
func ParseRefreshToken(token string) (*RefreshClaims, error) {
	tokenClaims, err := jwt.ParseWithClaims(token, &RefreshClaims{}, func(token *jwt.Token) (interface{}, error) {
		// 验证签名方法
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return jwtSecret, nil
	})

	if err != nil {
		return nil, err
	}

	claims, ok := tokenClaims.Claims.(*RefreshClaims)
	if !ok || !tokenClaims.Valid {
		return nil, errors.New("invalid refresh token")
	}

	return claims, nil
}

// ValidateAccessToken 验证访问令牌是否有效
func ValidateAccessToken(token string) (*Claims, error) {
	claims, err := ParseAccessToken(token)
	if err != nil {
		return nil, err
	}

	// 检查是否过期
	if claims.ExpiresAt < time.Now().Unix() {
		return nil, errors.New("token has expired")
	}

	return claims, nil
}

// ValidateRefreshToken 验证刷新令牌是否有效
func ValidateRefreshToken(token string) (*RefreshClaims, error) {
	claims, err := ParseRefreshToken(token)
	if err != nil {
		return nil, err
	}

	// 检查是否过期
	if claims.ExpiresAt < time.Now().Unix() {
		return nil, errors.New("refresh token has expired")
	}

	return claims, nil
}

// RefreshAccessToken 使用刷新令牌获取新的访问令牌
func RefreshAccessToken(refreshTokenStr string) (newAccessToken string, newRefreshToken string, err error) {
	// 验证刷新令牌
	refreshClaims, err := ValidateRefreshToken(refreshTokenStr)
	if err != nil {
		return "", "", err
	}

	// 这里应该从存储中验证original_jti是否仍然有效（未被撤销）
	// 为了简化，我们假设刷新令牌本身的有效性就足够了

	// 生成新的访问令牌和刷新令牌
	// 注意：在实际应用中，您可能需要从数据库或其他存储中获取用户的最新信息
	newAccessToken, newRefreshToken, err = GenerateTokenPair(
		refreshClaims.Uid,
		"", // 用户名可能需要从数据库中重新获取
		0,  // 用户类型可能需要从数据库中重新获取
		"", // 设备ID可能需要从原始请求中获取
		"", // 客户端IP可能需要从原始请求中获取
	)

	if err != nil {
		return "", "", err
	}

	return newAccessToken, newRefreshToken, nil
}

// GetUidFromToken 从令牌中提取用户ID
func GetUidFromToken(token string) (int64, error) {
	claims, err := ValidateAccessToken(token)
	if err != nil {
		return 0, err
	}
	return claims.Uid, nil
}

// GetUsernameFromToken 从令牌中提取用户名
func GetUsernameFromToken(token string) (string, error) {
	claims, err := ValidateAccessToken(token)
	if err != nil {
		return "", err
	}
	return claims.Username, nil
}

func GetTokenFormGinContext(c *gin.Context) string {
	authHeader := c.GetHeader(constant.GinHeaderTokenKey)
	if authHeader == "" {
		return ""
	}
	// 检查Bearer前缀
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return ""
	}

	return strings.TrimPrefix(authHeader, "Bearer ")

}
