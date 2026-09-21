package observability

import (
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// ServiceIdentity 是服务身份：链路资源属性与指标常量标签共用同一份来源
// （来自 runtime 配置的 name/id/version/env），避免两处各写一套。
type ServiceIdentity struct {
	// Name 是服务名（链路 service.name / 指标 service 标签）。
	Name string
	// ID 是实例 ID（链路 service.instance.id / 指标 service_instance_id 标签）。
	ID string
	// Version 是版本（链路 service.version / 指标 service_version 标签）。
	Version string
	// Env 是部署环境（链路 deployment.environment.name / 指标 env 标签）。
	Env string
}

// resourceAttrs 返回链路资源属性（semconv 命名；空值不注入）。
func (id ServiceIdentity) resourceAttrs() []attribute.KeyValue {
	attrs := []attribute.KeyValue{semconv.ServiceName(id.Name)}
	if id.Version != "" {
		attrs = append(attrs, semconv.ServiceVersion(id.Version))
	}
	if id.ID != "" {
		attrs = append(attrs, semconv.ServiceInstanceID(id.ID))
	}
	if id.Env != "" {
		attrs = append(attrs, semconv.DeploymentEnvironmentNameKey.String(id.Env))
	}
	return attrs
}

// metricLabels 返回 Prometheus 常量标签（空值不注入）：
// 短名与既有 service 标签风格一致；实例用 service_instance_id 而非 instance，
// 避免与 Prometheus 抓取目标自带的 instance 标签冲突（冲突会被改名为 exported_instance）。
func (id ServiceIdentity) metricLabels() map[string]string {
	labels := map[string]string{"service": id.Name}
	if id.Version != "" {
		labels["service_version"] = id.Version
	}
	if id.ID != "" {
		labels["service_instance_id"] = id.ID
	}
	if id.Env != "" {
		labels["env"] = id.Env
	}
	return labels
}
