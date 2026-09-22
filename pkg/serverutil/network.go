package serverutil

import (
	"github.com/huangyuCN/atlas-game-layout/pkg/enumconv"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
)

// Network 是监听网络类型（与 proto 枚举 Server.Network 一一对应）。
type Network int

// 网络类型取值（下标即枚举值，见 networkNames）。
const (
	NetworkTCP Network = iota
	NetworkTCP4
	NetworkTCP6
	NetworkUnix
)

// networkNames 是网络类型对应的 net.Listen 网络名（下标即枚举值）。
var networkNames = [...]string{
	NetworkTCP:  "tcp",
	NetworkTCP4: "tcp4",
	NetworkTCP6: "tcp6",
	NetworkUnix: "unix",
}

// String 返回 net.Listen 的网络名（越界返回空串，由调用方按未设置处理）。
func (n Network) String() string {
	if int(n) < 0 || int(n) >= len(networkNames) {
		return ""
	}
	return networkNames[n]
}

// networkOf 把 proto 网络枚举映射为 net.Listen 的网络名：未设置返回空串（交给底层默认），
// 未登记的枚举取值报错——静默回落到 tcp 会让「配了 tcp6 却监听在 tcp」难以发现。
func networkOf(n *configspb.Server_Network) (string, error) {
	if n == nil {
		return "", nil
	}
	mapped, err := enumconv.Map(*n, map[configspb.Server_Network]Network{
		configspb.Server_NETWORK_TCP:  NetworkTCP,
		configspb.Server_NETWORK_TCP4: NetworkTCP4,
		configspb.Server_NETWORK_TCP6: NetworkTCP6,
		configspb.Server_NETWORK_UNIX: NetworkUnix,
	}, "网络类型")
	if err != nil {
		return "", err
	}
	return mapped.String(), nil
}
