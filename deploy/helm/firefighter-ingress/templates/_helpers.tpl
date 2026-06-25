{{- define "firefighter-ingress.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride -}}
{{- else -}}
{{- .Chart.Name -}}
{{- end -}}
{{- end -}}

{{- define "firefighter-ingress.name" -}}
{{- default .Chart.Name .Values.nameOverride -}}
{{- end -}}

{{- define "firefighter-ingress.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version -}}
{{- end -}}

{{- define "firefighter-ingress.labels" -}}
app.kubernetes.io/name: {{ include "firefighter-ingress.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.Version }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}