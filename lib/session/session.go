// Package session 提供会话令牌的生成与格式校验。
// 令牌为 32 字节随机数的 hex 编码；与玩家绑定关系由 pkg/redis 存取。
package session

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// TokenLen 是令牌字节长度（hex 后 64 字符）。
const TokenLen = 32

// NewToken 生成随机会话令牌。
func NewToken() (string, error) {
	buf := make([]byte, TokenLen)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("session: 生成令牌失败: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// Valid 校验令牌格式（64 位小写 hex）。
func Valid(token string) bool {
	if len(token) != TokenLen*2 {
		return false
	}
	_, err := hex.DecodeString(token)
	return err == nil
}
