// Package gatewayv1 是 gateway 客户端协议包（唯一客户端命名空间，SDK 只消费本包）：
// 业务通道（注册/登录/心跳/登出/数据同步 + 匹配/组队 + 挤下线/失败/名册推送）
// 与战斗通道（加入战斗/帧输入/补帧 + 帧广播/结束推送）由 player_client.proto、
// match_client.proto、battle_client.proto 生成。
package gatewayv1
