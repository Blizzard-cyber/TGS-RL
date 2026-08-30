{{- define "tgsrl-operator.rbacIdentity" -}}
{{- $displayIdentity := printf "%s-%s" .Release.Name .Release.Namespace -}}
{{- $hashIdentity := printf "%s/%s" .Release.Name .Release.Namespace -}}
{{- $prefix := trunc 24 $displayIdentity | trimSuffix "-" -}}
{{- $digest := sha256sum $hashIdentity | trunc 12 -}}
{{- printf "%s-%s" $prefix $digest -}}
{{- end -}}

{{- define "tgsrl-operator.discoveryRBACName" -}}
{{- printf "%s-discovery" (include "tgsrl-operator.rbacIdentity" .) -}}
{{- end -}}

{{- define "tgsrl-operator.runtimeClassRBACName" -}}
{{- printf "%s-runtimeclass-writer" (include "tgsrl-operator.rbacIdentity" .) -}}
{{- end -}}
