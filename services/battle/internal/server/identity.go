package server

import (
	"context"
	"encoding/base64"
	"time"

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/stream"
	"github.com/huangyuCN/atlas/contrib/actor/frameops"
	"github.com/huangyuCN/atlas/contrib/actor/relay"
	"github.com/huangyuCN/atlas/contrib/edge/ticket"
	"github.com/huangyuCN/atlas/transport"
	"github.com/huangyuCN/atlas/transport/frame"
)

// FrameConn 是帧槽验票后的直连端口（由 battle 的直连连接注册表实现，装配期注入）：
// 登记本次直连并上报上线、判定对局是否已结束、向重连后的新连接补投结算结果。
type FrameConn interface {
	// Record 登记玩家的直连连接（首次登记与重连接管各上报一次上线）。
	Record(playerID string, c stream.Conn)
	// Ended 判定该对局是否已结束（结束留档未过期即真）：为真即在**投递之前**拒绝迟到 op，
	// 免得 SpawnAuto 重建空名单实例。
	Ended(battleID string) bool
	// ReplayEnd 向本次新连接补投留档的结算结果（幂等、有界；未留档/非参战返回 false）。
	ReplayEnd(playerID string, c stream.Conn) bool
}

// ticketIdentity 是 battle 帧面的身份解析器（frameops.IdentityResolver 实现）：
// 从帧请求头取会话槽（Atlas-Frame-Session，取值是 **base64url 编码的票据密文**），
// 验票通过即返回身份并登记本次直连连接（规格 §3.3：同一张票在接入层与 battle 两处用）；
// 对局**已结束**（留档未过期）时改为补投结果 + 稳定 reason 拒绝——绝不投递。
type ticketIdentity struct {
	key  []byte
	conn FrameConn
}

// NewTicketIdentity 构造帧槽验票身份解析器（key 为 32 字节 AEAD 密钥，装配期已校验长度）。
func NewTicketIdentity(key []byte, conn FrameConn) frameops.IdentityResolver {
	return &ticketIdentity{key: key, conn: conn}
}

// Resolve 解析一次帧请求的载体身份：无槽/解码失败/认证失败 → BATTLE_TICKET_INVALID；
// 已过期 → BATTLE_TICKET_EXPIRED（两个 reason 语义分开，SDK 据此决定重试还是重取票）；
// 已结束 → BATTLE_ENDED（语义是「该对局已结束」，SDK 据此停止发送而不是重试到超时）。
// **对局归属校验不在这里**——那要读协议正文里的 battle_id 并对参战名单，
// 属 battle actor 的职责（本解析器不解析协议正文）。
func (ti *ticketIdentity) Resolve(ctx context.Context, _ relay.RouteEntry) (frameops.Identity, error) {
	slot := sessionSlot(ctx)
	if slot == "" {
		return frameops.Identity{}, errorv1.ErrBattleTicketInvalid(
			"帧请求缺少会话槽 %s（直连需逐帧带票）", frame.RequestHeaderKeySession)
	}
	raw, err := base64.RawURLEncoding.DecodeString(slot)
	if err != nil {
		return frameops.Identity{}, errorv1.ErrBattleTicketInvalid("帧会话槽不是合法的 base64url 票据: %v", err)
	}
	tk, err := ticket.Decode(raw, ti.key)
	if err != nil {
		return frameops.Identity{}, errorv1.ErrBattleTicketInvalid("票据校验失败: %v", err)
	}
	if ticket.Expired(tk, time.Now()) {
		return frameops.Identity{}, errorv1.ErrBattleTicketExpired("票据已过期（玩家 %s）", tk.PlayerID)
	}
	// 已结束的对局：先补投留档结果（幂等、有界），再以稳定 reason 拒绝——**不登记、不投递**。
	// 投递会经 SpawnAuto 懒激活重建空名单实例，它恢复快照后立刻再次结算并关闭直连，
	// 客户端被反复丢弃（阶段 3 验收实测每轮 9–13 次 actor 起停）。
	if ti.conn.Ended(tk.BattleID) {
		ti.withConn(ctx, tk.BattleID, func(c stream.Conn) { ti.conn.ReplayEnd(tk.PlayerID, c) })
		return frameops.Identity{}, errorv1.ErrBattleEnded("对局 %s 已结束（玩家 %s）", tk.BattleID, tk.PlayerID)
	}
	ti.withConn(ctx, tk.BattleID, func(c stream.Conn) { ti.conn.Record(tk.PlayerID, c) })
	return frameops.Identity{PlayerID: tk.PlayerID, BattleID: tk.BattleID}, nil
}

// withConn 组装本次请求的直连句柄并交给 fn（登记与补投共用同一条「取句柄」口径）：
// 未注入直连端口或取不到连接句柄（非帧传输上下文的直调）即跳过——身份照样可用，
// 只是不参与直连推送/补投。
func (ti *ticketIdentity) withConn(ctx context.Context, battleID string, fn func(stream.Conn)) {
	if ti.conn == nil {
		return
	}
	c, ok := connOf(ctx, battleID)
	if !ok {
		return
	}
	fn(c)
}

// sessionSlot 从帧请求头取会话槽（无传输上下文或无该头返回空串，不臆造）。
func sessionSlot(ctx context.Context) string {
	tr, ok := transport.FromServerContext(ctx)
	if !ok || tr.RequestHeader() == nil {
		return ""
	}
	return tr.RequestHeader().Get(frame.RequestHeaderKeySession)
}

// connOf 从请求上下文组装本次连接的寻址句柄：数据报面（UDP）按对端键，
// 流式面（KCP/WS）按连接 ID；两者都取不到即返回 false。
func connOf(ctx context.Context, battleID string) (stream.Conn, bool) {
	kind := transport.Kind("")
	if tr, ok := transport.FromServerContext(ctx); ok {
		kind = tr.Kind()
	}
	c := stream.Conn{
		Kind:     kind,
		ConnID:   transport.ConnIDFromContext(ctx),
		Peer:     transport.PeerFromContext(ctx),
		BattleID: battleID,
	}
	if c.Peer != "" {
		return c, true
	}
	return c, c.ConnID != 0
}
