// Package admingamev1 是 game 的管理面协议包，由 admin.proto 生成：
// 承载内网 GM/运维的普通 gRPC 契约（AdminService）与统一请求信封（AdminContext）；
// 本包**只**有消息与标准 gRPC 产物，不含 actor/HTTP/客户端 SDK 产物（产物隔离见 admin_generation_test.go）。
package admingamev1
