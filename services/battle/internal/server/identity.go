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

// Recorder 是帧槽验票的直连登记端口（由 battle 的连接生命周期桥实现，装配期注入）：
// 登记本次直连并上报上线（连接未变化时不上报，去重在实现侧）。
type Recorder interface {
	// Record 登记玩家的直连连接（首次登记与重连接管各上报一次上线）。
	Record(playerID string, c stream.Conn)
}

// ticketIdentity 是 battle 帧面的身份解析器（frameops.IdentityResolver 实现）：
// 从帧请求头取会话槽（Atlas-Frame-Session，取值是 **base64url 编码的票据密文**），
// 验票通过即返回身份并登记本次直连连接（规格 §3.3：同一张票在接入层与 battle 两处用）。
type ticketIdentity struct {
	key []byte
	rec Recorder
}

// NewTicketIdentity 构造帧槽验票身份解析器（key 为 32 字节 AEAD 密钥，装配期已校验长度）。
func NewTicketIdentity(key []byte, rec Recorder) frameops.IdentityResolver {
	return &ticketIdentity{key: key, rec: rec}
}

// Resolve 解析一次帧请求的载体身份：无槽/解码失败/认证失败 → BATTLE_TICKET_INVALID；
// 已过期 → BATTLE_TICKET_EXPIRED（两个 reason 语义分开，SDK 据此决定重试还是重取票）。
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
	ti.register(ctx, tk)
	return frameops.Identity{PlayerID: tk.PlayerID, BattleID: tk.BattleID}, nil
}

// register 把本次连接登记进直连注册表并由桥上报上线（重连接管语义见 stream.Bridge）。
// 取不到连接句柄（非帧传输上下文的直调）即跳过：身份照样可用，只是不参与直连推送。
func (ti *ticketIdentity) register(ctx context.Context, tk ticket.Ticket) {
	if ti.rec == nil {
		return
	}
	c, ok := connOf(ctx, tk.BattleID)
	if !ok {
		return
	}
	ti.rec.Record(tk.PlayerID, c)
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
