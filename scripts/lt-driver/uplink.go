package main

import (
	"fmt"

	"github.com/huangyuCN/atlas/encoding"
	encodingproto "github.com/huangyuCN/atlas/encoding/proto"
)

// uplinkCodec 返回压测驱动上行请求使用的载荷编码 codec：**protobuf 二进制（ver=2）**。
//
// 帧引擎客户端会按 codec 标注请求帧头版本（proto → ver=2、json → ver=1，规范 §3.1
// 载荷编码协商），服务端按同一个 ver 选 codec。生产 profile 里入向解码是 battle 的
// 固定成本（frameops.decodeRequest cum 8.7%），切二进制后从 protojson 的反射解析
// 换成按字段号直接落位，分配与 CPU 都大幅下降。
//
// 兼容期服务端仍能解 ver=1：本函数返回错误而不是静默退回 json——压测工具若悄悄
// 发 JSON，量到的就还是旧口径（宁可启动即失败）。
func uplinkCodec() (encoding.Codec, error) {
	c := encoding.GetCodec(encodingproto.Name)
	if c == nil {
		return nil, fmt.Errorf("protobuf codec 未注册（%q）：压测驱动要求 ver=2 二进制上行", encodingproto.Name)
	}
	return c, nil
}
