# -*- mode: Python -*-
# Tiltfile — one command (tilt up) to run the entire scan pipeline in Kubernetes.

# ── Build custom images from existing Dockerfiles ──────────────────────────
# Tilt watches the build context for file changes and automatically rebuilds
# the affected image + redeploys the pod.
docker_build('processor', '.', dockerfile='cmd/processor/Dockerfile', only=['./cmd/processor/', './pkg/', './go.mod', './go.sum'])
docker_build('scanner', '.', dockerfile='cmd/scanner/Dockerfile', only=['./cmd/scanner/', './pkg/', './go.mod', './go.sum'])

# ── Generate dashboard ConfigMap from existing JSON ────────────────────────
# This avoids duplicating the 200+ line dashboard JSON in a YAML manifest.
# --dry-run=client produces YAML without hitting the cluster; Tilt applies it.
k8s_yaml(local(
    'kubectl create configmap grafana-dashboards'
    + ' --from-file=scan-processor.json=config/grafana/dashboards/scan-processor.json'
    + ' --dry-run=client -o yaml'
))

# ── Deploy all Kubernetes manifests ────────────────────────────────────────
k8s_yaml([
    'k8s/pubsub.yaml',
    'k8s/pubsub-init.yaml',
    'k8s/postgres.yaml',
    'k8s/scanner.yaml',
    'k8s/processor.yaml',
    'k8s/prometheus.yaml',
    'k8s/grafana.yaml',
])

# ── Resource configuration ─────────────────────────────────────────────────

# Infrastructure: the emulator and postgres must be ready before dependent
# services start.
k8s_resource('pubsub', labels=['infrastructure'])
k8s_resource(
    'pubsub-init',
    resource_deps=['pubsub'],
    labels=['infrastructure'],
)
k8s_resource(
    'postgres',
    port_forwards='5432:5432',
    labels=['infrastructure'],
)

# Pipeline: scanner and processor both require the topic/subscription to exist.
k8s_resource(
    'scanner',
    resource_deps=['pubsub-init'],
    labels=['mini-scan'],
)
k8s_resource(
    'processor',
    port_forwards='8080:8080',
    resource_deps=['pubsub-init', 'postgres'],
    labels=['mini-scan'],
)

# Observability: prometheus and grafana can start independently.
k8s_resource(
    'prometheus',
    port_forwards='9090:9090',
    labels=['observability'],
)
k8s_resource(
    'grafana',
    port_forwards='3000:3000',
    objects=['grafana-dashboards:configmap'],
    labels=['observability'],
)
