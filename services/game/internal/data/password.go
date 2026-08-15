// Package data 提供 game 服务的数据层：
// models（玩家聚合根）、repo（缓存与持久化）与共享辅助（口令摘要）。
package data

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
)

// saltLen 是密码盐的字节长度。
const saltLen = 16

// HashPassword 生成口令摘要：随机盐 + sha256(salt + password)，
// 返回盐与摘要的 hex 编码。示例实现；生产环境建议 bcrypt/argon2。
func HashPassword(password string) (salt, hash string, err error) {
	if password == "" {
		return "", "", fmt.Errorf("data: 口令不能为空")
	}
	buf := make([]byte, saltLen)
	if _, err := rand.Read(buf); err != nil {
		return "", "", fmt.Errorf("data: 生成盐失败: %w", err)
	}
	salt = hex.EncodeToString(buf)
	return salt, digest(salt, password), nil
}

// VerifyPassword 常量时间比较校验口令（防时序侧信道）。
func VerifyPassword(password, salt, hash string) bool {
	if password == "" || salt == "" || hash == "" {
		return false
	}
	got := digest(salt, password)
	return subtle.ConstantTimeCompare([]byte(got), []byte(hash)) == 1
}

// digest 计算 sha256(salt + password) 的 hex 摘要。
func digest(salt, password string) string {
	sum := sha256.Sum256([]byte(salt + password))
	return hex.EncodeToString(sum[:])
}
