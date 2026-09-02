package common

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"
)

const (
	TokenSecret      = "game" //加密密钥
	TokenInvalidTime = 24     //小时 - 访问令牌有效期
	RefreshTokenTime = 7 * 24 //小时 - 刷新令牌有效期
)

type CustomClaims interface {
	jwt.Claims
	GetId() string
	GetUserId() int64
	GetUsername() string
	GetDeviceId() string
	SetJwtClaims(claims jwt.Claims)
}
type CustomClaimsImpl struct {
	UserId   int64  `json:"userId"`
	UserName string `json:"userName"`
	ClientIp string `json:"client_ip"` // 客户端IP
	jwt.RegisteredClaims
}

func (c *CustomClaimsImpl) GetId() string       { return c.ID }
func (c *CustomClaimsImpl) GetUserId() int64    { return c.UserId }
func (c *CustomClaimsImpl) GetUsername() string { return c.UserName }
func (c *CustomClaimsImpl) SetJwtClaims(claims jwt.Claims) {
	c.RegisteredClaims = claims.(jwt.RegisteredClaims)
}

type Claims struct {
	CustomClaimsImpl
	UserType int    `json:"user_type"`
	DeviceId string `json:"device_id"` // 设备ID
}

func (c *Claims) GetDeviceId() string { return c.DeviceId }

// RefreshClaims 刷新令牌的声明
type RefreshClaims struct {
	Uid         int64  `json:"uid"`
	DeviceId    string `json:"device_id"`    // 设备ID
	OriginalJti string `json:"original_jti"` // 原始访问令牌的JTI
	jwt.StandardClaims
}

var jwtSecret = []byte(TokenSecret)

func GenerateClaimsToken(claims jwt.Claims) (string, error) {
	tokenClaims := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token, err := tokenClaims.SignedString(jwtSecret)
	if err != nil {
		return "", err
	}

	return token, nil
}

// GenerateAccessToken 生成访问令牌
func GenerateAccessToken(uid int64, username string, userType int, deviceId string, clientIp string) (string, error) {
	now := time.Now()
	expireTime := now.Add(time.Duration(TokenInvalidTime) * time.Hour)

	claims := Claims{
		CustomClaimsImpl: CustomClaimsImpl{
			UserId:   uid,
			UserName: username,
			ClientIp: clientIp,
		},
		UserType: userType,
		DeviceId: deviceId,
	}
	claims.SetJwtClaims(jwt.RegisteredClaims{
		ExpiresAt: jwt.NewNumericDate(expireTime),
		IssuedAt:  jwt.NewNumericDate(now),
		NotBefore: jwt.NewNumericDate(now),
		Issuer:    "common-jwt-service",
		Subject:   fmt.Sprintf("user_%d", uid),
		ID:        uuid.NewString(),
	})

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

func GenerateTokenPairWithClaims(claims CustomClaims) (accessToken string, refreshToken string, err error) {
	now := time.Now()
	expireTime := now.Add(time.Duration(TokenInvalidTime) * time.Hour)

	claims.SetJwtClaims(jwt.StandardClaims{
		ExpiresAt: expireTime.Unix(),
		IssuedAt:  now.Unix(),
		NotBefore: now.Unix(),
		Issuer:    "common-jwt-service",
		Subject:   fmt.Sprintf("user_%d", claims.GetUserId()),
		Id:        uuid.NewString(),
	})

	accessToken, err = GenerateClaimsToken(claims)
	if err != nil {
		return
	}

	// 解析访问令牌获取JTI
	claim, err := ParseAccessTokenWithClaims(accessToken, claims)
	if err != nil {
		return
	}

	refreshToken, err = GenerateRefreshToken(claims.GetUserId(), claim.GetId(), claims.GetDeviceId())
	if err != nil {
		return
	}

	return
}

// GenerateTokenPair 生成访问令牌和刷新令牌对
func GenerateTokenPair(uid int64, username string, userType int, deviceId string, clientIp string) (accessToken string, refreshToken string, err error) {
	accessToken, err = GenerateAccessToken(uid, username, userType, deviceId, clientIp)
	if err != nil {
		return
	}

	// 解析访问令牌获取JTI
	claims, err := ParseAccessTokenWithClaims(accessToken, &Claims{})
	if err != nil {
		return
	}

	refreshToken, err = GenerateRefreshToken(uid, claims.ID, deviceId)
	if err != nil {
		return
	}

	return accessToken, refreshToken, nil
}

// ParseAccessToken 解析访问令牌（使用默认Claims类型）
func ParseAccessToken(token string) (*Claims, error) {
	return ParseAccessTokenWithClaims(token, &Claims{})
}

// ParseAccessTokenWithClaims 解析访问令牌，支持自定义Claims实体
// claims参数需要传入一个实现了CustomClaims接口的空实例，解析结果将写入该实例
func ParseAccessTokenWithClaims[T CustomClaims](token string, claims T) (T, error) {
	tokenClaims, err := jwt.ParseWithClaims(token, claims, func(token *jwt.Token) (interface{}, error) {
		// 验证签名方法
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return jwtSecret, nil
	})

	if err != nil {
		var zero T
		return zero, err
	}

	c, ok := tokenClaims.Claims.(T)
	if !ok || !tokenClaims.Valid {
		var zero T
		return zero, errors.New("invalid token")
	}

	return c, nil
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

// ValidateAccessToken 验证访问令牌是否有效（使用默认Claims类型）
func ValidateAccessToken(token string) (*Claims, error) {
	return ValidateAccessTokenWithClaims(token, &Claims{})
}

// ValidateAccessTokenWithClaims 验证访问令牌是否有效，支持自定义Claims实体
func ValidateAccessTokenWithClaims[T CustomClaims](token string, claims T) (T, error) {
	parsed, err := ParseAccessTokenWithClaims(token, claims)
	if err != nil {
		var zero T
		return zero, err
	}

	return parsed, nil
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
	return claims.UserId, nil
}

// GetUsernameFromToken 从令牌中提取用户名
func GetUsernameFromToken(token string) (string, error) {
	claims, err := ValidateAccessToken(token)
	if err != nil {
		return "", err
	}
	return claims.UserName, nil
}

func GetTokenFormGinContext(c *gin.Context) string {
	authHeader := c.GetHeader(GinHeaderTokenKey)
	if authHeader == "" {
		return ""
	}
	// 检查Bearer前缀
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return ""
	}

	return strings.TrimPrefix(authHeader, "Bearer ")

}
