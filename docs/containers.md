# Containers and Kubernetes

English | [简体中文](zh-CN/containers.md)

These examples run **one node** for evaluation and controlled deployments. They
do not establish high availability or remove the [known limitations](limitations.md).
The image contains the engine, example plugin, static UI and license notices; it
runs as UID/GID 10001. It includes `procps` for resource monitoring and CA/timezone
data. Build it locally; no published registry image is assumed:

```sh
docker build -t owlcron:local .
docker volume create owlcron-state
```

Supply `OWLCRON_SECRET_KEY` through your secret manager or a mode-0600 environment
file. Do not put the actual secret in command history. For example, use
`--env-file /secure/owlcron.env` in both invocations below:

```sh
docker run --rm --env-file /secure/owlcron.env \
  -v owlcron-state:/var/lib/owlcron owlcron:local setup
docker run -d --name owlcron --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=64m --cap-drop ALL \
  --security-opt no-new-privileges --env-file /secure/owlcron.env \
  -v owlcron-state:/var/lib/owlcron -p 127.0.0.1:3012:3012 owlcron:local
```

Mount a replacement configuration at `/etc/owlcron/config.json:ro` when changing
the public URL or other settings. Bind-mounted state must be writable by UID
10001; named volumes inherit the image directory ownership. Persist the whole
`/var/lib/owlcron`, including logs and queues. Stop with `docker stop -t 90 owlcron`.
Back up while stopped; removing the container must not remove its state volume.
See [administrator initialization and HTTPS](deployment.md) before exposing it.
Without an initial password, retrieve the sensitive bootstrap file from
`/var/lib/owlcron/data/bootstrap-admin.json` through a trusted local terminal,
verify login and delete it. Do not put this file in support logs or CI artifacts.

## Kubernetes

The manifest is JSON, a format accepted by `kubectl apply -f`. It uses current
`apps/v1` and core `v1` resources, a single-replica Recreate deployment, a 10 GiB
ReadWriteOnce PVC and a ClusterIP service. It requests no API token and uses a
read-only root filesystem, a non-root identity, dropped capabilities, probes and
a 90-second termination grace period. A default StorageClass must be available.

1. Publish the locally built image to a registry your cluster can access, using
   an immutable version or digest. Replace **both** `owlcron:local` image fields
   in `deploy/kubernetes/owlcron.json`. Do not apply the example unchanged unless
   that exact local image has been loaded into your development cluster.
2. In the target namespace, create `owlcron-secrets` with a `secret-key` entry
   from a protected file: `kubectl create secret generic owlcron-secrets
   --from-file=secret-key=/secure/owlcron-secret-key`. The file must contain a
   random value of at least 32 bytes. No secret is included in the manifest.
3. Review the ConfigMap, storage size/class and resource budget; then apply:

```sh
kubectl apply -f deploy/kubernetes/owlcron.json
kubectl rollout status deployment/owlcron
kubectl port-forward service/owlcron 3012:3012
```

The init container runs idempotent storage setup. Use a trusted terminal to
retrieve and remove the initial bootstrap file after validating login. The
service is internal only: configure HTTPS and trusted proxy CIDRs before adding
an ingress. Apply network policy for your actual ingress, DNS, SMTP and WebHook
requirements; this example does not install an allow-all policy or claim to
isolate a cluster whose default networking permits traffic. Do not scale this
filesystem deployment above one replica. Moving to a cluster requires independent
validation of shared-storage semantics, TLS, identity and failure recovery.

CI builds and boots the image with persistent state, restarts it, and checks the
deployment's offline safety invariants. This is not a live Kubernetes certification;
validate scheduling, volume ownership and policy on the target cluster.

## Health probes

`GET /api/app/ping` is a liveness check. `GET /api/app/ready` (also available
with `/v1`) returns HTTP 200 after local startup completes and storage can
answer a read-only existence check, or HTTP 503 while starting, draining,
closed, or when the check fails or exceeds two seconds. Both are public and
readiness returns only a generic result, without backend error details.
A healthy worker does not need to hold leadership to be ready. Concurrent
requests share a storage probe; a stuck backend cannot accumulate probe tasks.
Startup writes a one-byte marker to the reserved `global/readiness_probe` key
before starting services, with a two-second deadline. Storage permissions must
allow writing and reading this key. Subsequent probes only check this small
marker, without reading application state or writing any data; a missing marker
is unhealthy. All nodes share the same constant marker, which remains on shutdown.
A backend result arriving after the two-second probe deadline remains a failure,
even if it reports success.

The Kubernetes example uses readiness for startup and service routing, and
liveness for restarts. A storage outage therefore removes the pod from service
without causing a restart loop. Readiness checks connectivity and read access;
it does not certify write permission, free disk space, quorum, job execution,
or the durability guarantees described in [known limitations](limitations.md).
