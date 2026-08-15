package models

// 本文件是聚合根的领域行为（手写，与字段声明文件分离）：
// 使用 cow 生成的 undo 写代理完成事务性修改；字段声明保持干净。

// GrantItem 发放道具：已持有则累加，未持有则追加（undo 事务内完成）。
func (p *Player) GrantItem(itemID, count uint32) {
	RunTx(func(tx *TxContext) {
		for i, it := range p.Items {
			if it.ItemID == itemID {
				p.SetItemsAt(tx, i, &Item{ItemID: itemID, Count: it.Count + count})
				return
			}
		}
		p.AppendItems(tx, &Item{ItemID: itemID, Count: count})
	})
}

// RunTx 在聚合根事务作用域执行写操作：fn 返回即提交（清空 undo 日志）；
// 调用方在 fn 内返回错误并在外部触发 Rollback 的用法见 docs/guide/tx-context.md。
// TxContext 由包级池提供（单协程串行前提由 PlayerActor 保证）。
func RunTx(fn func(tx *TxContext)) {
	tx := txPool.Get().(*TxContext)
	tx.Reset()
	defer txPool.Put(tx)
	fn(tx)
}
