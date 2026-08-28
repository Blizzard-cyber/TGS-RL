{{- define "tgsrl-operator.runtimeClassRBACName" -}}
{{- $displayIdentity := printf "%s-%s" .Release.Name .Release.Namespace -}}
{{- $hashIdentity := printf "%s/%s" .Release.Name .Release.Namespace -}}
{{- $prefix := trunc 30 $displayIdentity | trimSuffix "-" -}}
{{- $digest := sha256sum $hashIdentity | trunc 12 -}}
{{- printf "%s-runtimeclass-writer-%s" $prefix $digest -}}
{{- end -}}
