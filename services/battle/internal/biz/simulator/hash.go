package simulator

// 帧级状态哈希：定向二进制编码 + FNV-1a 64。
//
// 为什么单独一条编码（不复用 Snapshot 的 json.Marshal）：前者要产出**可还原**的状态字节，
// 必须走 JSON（反射 + 每帧 23 次分配、约 2.2µs）；帧级哈希每帧都要算，只需「同状态同值、
// 状态变则值变」，故按 State 的字段定向编码，零反射零分配。
//
// 编码口径（与 hash_test.go 的参考实现逐字节一致）：
//
//	玩家数 | (每个玩家: 长度前缀 + 名字 + 位置 int32 定长 8 字节)
//	玩家数 | (每个玩家: 长度前缀 + 名字 + 得分 int32 定长 8 字节)
//	长度前缀 + 胜者
//
// 玩家名按字典序（map 迭代顺序随机，不排序就不可复现）；整数一律小端 8 字节。
// 注意：本口径与 Snapshot().Hash（JSON 口径）**数值不同**，两者各自稳定；
// 快照帧的 SnapshotMeta.StateHash 仍走 JSON 口径，帧级 StateHash 走本口径。
const (
	// fnvOffset64 是 FNV-1a 64 的初始值（与 hash/fnv 同参数）。
	fnvOffset64 = 14695981039346656037
	// fnvPrime64 是 FNV-1a 64 的质数。
	fnvPrime64 = 1099511628211
	// maxInlineKeys 是栈上键缓冲容量：常见对局 2~8 人，超出时 append 自行扩容（多一次分配）。
	maxInlineKeys = 8
)

// Hash 返回当前状态的轻量哈希（模拟器实现 lockstep.StateHasher 的入口，每帧调用）。
//
// 覆盖 State 的全部字段（位置/得分/胜者）：任一字段变化即变值，防「定向编码漏字段」。
// 同状态同值（与 map 迭代顺序、恢复前后的键序无关）。
func (b *Battle) Hash() uint64 {
	var buf [maxInlineKeys]string
	keys := buf[:0]
	for p := range b.pos {
		keys = append(keys, p)
	}
	for p := range b.score {
		if !hasKey(keys, p) {
			keys = append(keys, p)
		}
	}
	sortKeys(keys)
	h := hashU64(fnvOffset64, uint64(len(keys)))
	for _, p := range keys {
		h = hashString(h, p)
		h = hashU64(h, uint64(uint32(b.pos[p])))
	}
	h = hashU64(h, uint64(len(keys)))
	for _, p := range keys {
		h = hashString(h, p)
		h = hashU64(h, uint64(uint32(b.score[p])))
	}
	return hashString(h, b.winner)
}

// hasKey 判断键是否已在切片中（键数极少，线性扫描比建集合更省且零分配）。
func hasKey(keys []string, k string) bool {
	for _, x := range keys {
		if x == k {
			return true
		}
	}
	return false
}

// sortKeys 对键做插入排序（键数极少；避免排序库带来的切片逃逸分配）。
func sortKeys(keys []string) {
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
}

// hashU64 把整数按 8 字节小端并入 FNV-1a 64（无临时缓冲）。
func hashU64(h, v uint64) uint64 {
	for i := 0; i < 8; i++ {
		h ^= v & 0xff
		h *= fnvPrime64
		v >>= 8
	}
	return h
}

// hashString 把「长度前缀 + 内容」并入 FNV-1a 64（长度前缀避免相邻串拼接歧义）。
func hashString(h uint64, s string) uint64 {
	h = hashU64(h, uint64(len(s)))
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= fnvPrime64
	}
	return h
}
