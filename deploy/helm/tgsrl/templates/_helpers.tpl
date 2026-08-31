{{- define "tgsrl.image" -}}
{{- $image := index . 0 -}}
{{- if $image.digest -}}
{{- printf "%s@%s" $image.repository $image.digest -}}
{{- else -}}
{{- printf "%s:%s" $image.repository $image.tag -}}
{{- end -}}
{{- end -}}

{{- define "tgsrl.securityContext" -}}
{{ toYaml .Values.global.securityContext }}
{{- end -}}

{{- define "tgsrl.podSecurityContext" -}}
{{ toYaml .Values.global.podSecurityContext }}
{{- end -}}

{{- define "tgsrl.stateVolume" -}}
{{- $root := index . 0 -}}
{{- $name := index . 1 -}}
{{- $persistence := index . 2 -}}
{{- if $persistence.enabled }}
persistentVolumeClaim:
  claimName: {{ default (printf "%s-state" $name) $persistence.existingClaim }}
{{- else }}
emptyDir: {}
{{- end }}
{{- end -}}
