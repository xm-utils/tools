package common

import (
	"crypto"
	"crypto/md5"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
)

// RsaUtil RSA工具结构体，支持PKCS8 + SHA256
type RsaUtil struct {
	privateKey *rsa.PrivateKey
	publicKey  *rsa.PublicKey
}

// NewRsaUtil 通过PEM格式的私钥和公钥创建RsaUtil实例
// privateKeyPem: PEM格式私钥（PKCS8），可为空
// publicKeyPem: PEM格式公钥，可为空
func NewRsaUtil(privateKeyPem, publicKeyPem string) (*RsaUtil, error) {
	r := &RsaUtil{}

	if privateKeyPem != "" {
		key, err := ParsePrivateKey(privateKeyPem)
		if err != nil {
			return nil, fmt.Errorf("解析私钥失败: %w", err)
		}
		r.privateKey = key
	}

	if publicKeyPem != "" {
		key, err := ParsePublicKey(publicKeyPem)
		if err != nil {
			return nil, fmt.Errorf("解析公钥失败: %w", err)
		}
		r.publicKey = key
	}

	return r, nil
}

// GenerateKeyPair 生成RSA密钥对（PKCS8格式）
// bits: 密钥长度，推荐2048或4096
func GenerateKeyPair(bits int) (privateKeyPem, publicKeyPem string, err error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return "", "", fmt.Errorf("生成RSA密钥对失败: %w", err)
	}

	// 私钥使用PKCS8格式编码
	privBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return "", "", fmt.Errorf("PKCS8编码私钥失败: %w", err)
	}
	privBlock := &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: privBytes,
	}
	privateKeyPem = string(pem.EncodeToMemory(privBlock))

	// 公钥编码
	pubBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		return "", "", fmt.Errorf("编码公钥失败: %w", err)
	}
	pubBlock := &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubBytes,
	}
	publicKeyPem = string(pem.EncodeToMemory(pubBlock))

	return privateKeyPem, publicKeyPem, nil
}

// ParsePrivateKey 解析PEM格式的PKCS8私钥
// 支持传入完整PEM格式或纯Base64内容，纯Base64会自动补全PEM头尾
func ParsePrivateKey(pemStr string) (*rsa.PrivateKey, error) {
	pemStr = strings.TrimSpace(pemStr)

	// 检查是否包含PEM头部，如果没有则自动补全
	if !strings.Contains(pemStr, "-----BEGIN") {
		pemStr = "-----BEGIN PRIVATE KEY-----\n" + pemStr + "\n-----END PRIVATE KEY-----"
	}

	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		return nil, errors.New("PEM解码失败，未找到有效的PEM块")
	}

	// 尝试PKCS8格式
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("解析PKCS8私钥失败: %w", err)
	}

	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("密钥类型不是RSA")
	}
	return rsaKey, nil
}

// ParsePublicKey 解析PEM格式的公钥
// 支持传入完整PEM格式或纯Base64内容，纯Base64会自动补全PEM头尾
func ParsePublicKey(pemStr string) (*rsa.PublicKey, error) {
	pemStr = strings.TrimSpace(pemStr)

	// 检查是否包含PEM头部，如果没有则自动补全
	if !strings.Contains(pemStr, "-----BEGIN") {
		pemStr = "-----BEGIN PUBLIC KEY-----\n" + pemStr + "\n-----END PUBLIC KEY-----"
	}

	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		return nil, errors.New("PEM解码失败，未找到有效的PEM块")
	}

	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("解析公钥失败: %w", err)
	}

	rsaPub, ok := pub.(*rsa.PublicKey)
	if !ok {
		return nil, errors.New("密钥类型不是RSA")
	}
	return rsaPub, nil
}

// Encrypt 公钥加密（OAEP + SHA256）
// plaintext: 明文字符串
// 返回: Base64编码的密文
func (r *RsaUtil) Encrypt(plaintext string) (string, error) {
	if r.publicKey == nil {
		return "", errors.New("公钥未设置")
	}
	return EncryptWithPublicKey([]byte(plaintext), r.publicKey)
}

// Decrypt 私钥解密（OAEP + SHA256）
// ciphertext: Base64编码的密文
// 返回: 明文字节切片
func (r *RsaUtil) Decrypt(ciphertext string) ([]byte, error) {
	if r.privateKey == nil {
		return nil, errors.New("私钥未设置")
	}
	return DecryptWithPrivateKey(ciphertext, r.privateKey)
}

// DecryptToString 私钥解密并返回字符串
func (r *RsaUtil) DecryptToString(ciphertext string) (string, error) {
	data, err := r.Decrypt(ciphertext)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// Sign 使用私钥签名（PKCS1v15 + SHA256）
// data: 待签名的数据
// 返回: Base64编码的签名
func (r *RsaUtil) Sign(data []byte) (string, error) {
	if r.privateKey == nil {
		return "", errors.New("私钥未设置")
	}
	return SignWithPrivateKey(data, r.privateKey)
}

// SignString 对字符串进行签名
func (r *RsaUtil) SignString(data string) (string, error) {
	return r.Sign([]byte(data))
}

// Verify 使用公钥验签（PKCS1v15 + SHA256）
// data: 原始数据
// signature: Base64编码的签名
func (r *RsaUtil) Verify(data []byte, signature string) error {
	if r.publicKey == nil {
		return errors.New("公钥未设置")
	}
	return VerifyWithPublicKey(data, signature, r.publicKey)
}

// VerifyString 对字符串进行验签
func (r *RsaUtil) VerifyString(data string, signature string) error {
	return r.Verify([]byte(data), signature)
}

// ============ 独立函数（无需创建实例即可使用） ============

// EncryptWithPublicKey 公钥加密（OAEP + SHA256）
func EncryptWithPublicKey(plaintext []byte, pub *rsa.PublicKey) (string, error) {
	ciphertext, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, pub, plaintext, nil)
	if err != nil {
		return "", fmt.Errorf("RSA加密失败: %w", err)
	}
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// DecryptWithPrivateKey 私钥解密（OAEP + SHA256）
func DecryptWithPrivateKey(ciphertextBase64 string, priv *rsa.PrivateKey) ([]byte, error) {
	ciphertext, err := base64.StdEncoding.DecodeString(ciphertextBase64)
	if err != nil {
		return nil, fmt.Errorf("Base64解码失败: %w", err)
	}

	plaintext, err := rsa.DecryptOAEP(sha256.New(), rand.Reader, priv, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("RSA解密失败: %w", err)
	}
	return plaintext, nil
}

// SignWithPrivateKey 使用私钥签名（PKCS1v15 + SHA256）
func SignWithPrivateKey(data []byte, priv *rsa.PrivateKey) (string, error) {
	hash := sha256.Sum256(data)
	signature, err := rsa.SignPKCS1v15(rand.Reader, priv, crypto.SHA256, hash[:])
	if err != nil {
		return "", fmt.Errorf("RSA签名失败: %w", err)
	}
	return base64.StdEncoding.EncodeToString(signature), nil
}

// VerifyWithPublicKey 使用公钥验签（PKCS1v15 + SHA256）
func VerifyWithPublicKey(data []byte, signatureBase64 string, pub *rsa.PublicKey) error {
	signature, err := base64.StdEncoding.DecodeString(signatureBase64)
	if err != nil {
		return fmt.Errorf("Base64解码签名失败: %w", err)
	}

	hash := sha256.Sum256(data)
	err = rsa.VerifyPKCS1v15(pub, crypto.SHA256, hash[:], signature)
	if err != nil {
		return fmt.Errorf("验签失败: %w", err)
	}
	return nil
}

// EncryptToBase64 公钥加密，输入纯字符串，返回Base64密文
func EncryptToBase64(plaintext string, pub *rsa.PublicKey) (string, error) {
	return EncryptWithPublicKey([]byte(plaintext), pub)
}

// DecryptFromBase64 私钥解密，输入Base64密文，返回纯字符串
func DecryptFromBase64(ciphertextBase64 string, priv *rsa.PrivateKey) (string, error) {
	plaintext, err := DecryptWithPrivateKey(ciphertextBase64, priv)
	if err != nil {
		return "", err
	}
	return string(plaintext), nil
}

// 生成32位MD5
func MD5(text string) string {
	ctx := md5.New()
	ctx.Write([]byte(text))
	return hex.EncodeToString(ctx.Sum(nil))
}
