// 直连连接生命周期事件（规格 §9.1）：由帧引擎钩子经 stream.Bridge 翻译成事件对象，
// 非阻塞投递到战斗 actor 后由其掉线策略消费（battle_offline.go）。
// 事件只携带玩家身份——对局由 actor 实例自身确定（PID = battle:<battleID>）。

package biz

// PlayerOnline 是直连上线事件（帧槽验票登记成功后上报）：
// 首登与重连接管各一条，逐帧重复登记不重复上报（去重在 stream.Bridge）。
type PlayerOnline struct {
	// PlayerID 是上线玩家（身份来自票面，不由消息体携带来源地址）。
	PlayerID string
}

// PlayerOffline 是直连断开事件（帧引擎连接生命周期事件按端点反查玩家后上报；未登记端点不发）。
// 是否过期（玩家已重连）不由上报方判定，由战斗 actor 复核注册表决定（规格 §9.2 硬约束②）。
type PlayerOffline struct {
	// PlayerID 是断开玩家。
	PlayerID string
}
