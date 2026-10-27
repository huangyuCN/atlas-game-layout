// 游戏协议包的 Go module：只依赖 protobuf 运行时（google.golang.org/protobuf）。
//
// 生成物在 api/ 下（由 scripts/gen-protocol.sh 产出，勿手改）；模块路径与目录
// protocol/go/ 对应，import 形如 .../protocol/api/battle/v1。
module github.com/huangyuCN/atlas-game-layout/protocol

go 1.26.5

require google.golang.org/protobuf v1.36.12
