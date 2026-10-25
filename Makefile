.PHONY: build test race vet frontend frontend-test integration redis-integration redis-tls-integration redis-mtls-integration redis-acl-integration redis-storage-faults process-integration process-integration-repeat linux-acceptance remote-storage-faults remote-process-integration linux-load-acceptance capacity-acceptance soak-acceptance cluster-load-acceptance fmt-check licenses-check licenses-update docs-check smoke release

docs-check:
	python3 tests/check_docs.py

fmt-check:
	@files="$$(find . -name '*.go' -not -path './htdocs-deps/node_modules/*' -not -path './.git/*' -exec gofmt -l {} +)" || exit 1; test -z "$$files" || { printf '%s\n' "$$files"; exit 1; }

licenses-check:
	python3 tests/check_licenses.py

licenses-update:
	python3 tests/check_licenses.py --write

# Build frontend and both executables before invoking the isolated installation check.
smoke:
	python3 tests/smoke_install.py

GO_BUILD_FLAGS ?=
LDFLAGS ?=
DIST_DIR ?= dist
# Treat release values as data; never expand them through the recipe shell.
unexport VERSION DIST_DIR

build:
	go build $(GO_BUILD_FLAGS) -ldflags "$(LDFLAGS)" -o owlcron-engine .
	go build $(GO_BUILD_FLAGS) -o owlcron-script ./cmd/owlcron-script
	cd bin/echo-plugin && go build $(GO_BUILD_FLAGS) -o echo-plugin .

release: export OWLCRON_RELEASE_VERSION := $(value VERSION)
release: export OWLCRON_RELEASE_OUTPUT := $(value DIST_DIR)
release:
	python3 tools/package_release.py

test:
	go test ./... -count=1 -timeout=360s

race:
	go test -race ./... -count=1 -timeout=360s

vet:
	go vet ./...

frontend:
	cd htdocs-deps && bun build.js dist

frontend-test:
	bun test tests/*_ui.test.cjs tests/bun_lock.test.mjs

integration:
	@test -n "$$OWLCRON_TEST_S3_ENDPOINT" || (echo "Set OWLCRON_TEST_S3_ENDPOINT" >&2; exit 1)
	@test -n "$$OWLCRON_TEST_COUCHBASE_CONNECTION" || (echo "Set OWLCRON_TEST_COUCHBASE_CONNECTION" >&2; exit 1)
	@test -n "$$OWLCRON_TEST_REDIS_ADDR" || (echo "Set OWLCRON_TEST_REDIS_ADDR" >&2; exit 1)
	go test -race ./... -count=1 -timeout=240s

# Explicit Redis entry points fail when the real service is missing.
redis-integration:
	@test -n "$$OWLCRON_TEST_REDIS_ADDR" || (echo "Set OWLCRON_TEST_REDIS_ADDR" >&2; exit 1)
	go test -race ./storage ./cli -run '^TestRedisReal' -count=1 -timeout=360s
	go test -race ./api ./engine -run '^Test(SingleNodeSystemLifecycle|ClusterAuthenticatedDispatchAndReconnect|PrimaryOutageAutomaticTakeoverPreservesCursor)$$/^Redis$$' -count=1 -timeout=240s

redis-tls-integration:
	@test -n "$$OWLCRON_TEST_REDIS_TLS_ADDR" && test -n "$$OWLCRON_TEST_REDIS_TLS_CA_FILE" || (echo "Set OWLCRON_TEST_REDIS_TLS_ADDR and OWLCRON_TEST_REDIS_TLS_CA_FILE" >&2; exit 1)
	go test -race ./storage -run '^TestRedisRealTLSConnection$$' -count=1 -timeout=120s

redis-mtls-integration:
	@test -n "$$OWLCRON_TEST_REDIS_TLS_ADDR" && test -n "$$OWLCRON_TEST_REDIS_TLS_CA_FILE" && test -n "$$OWLCRON_TEST_REDIS_TLS_CERT_FILE" && test -n "$$OWLCRON_TEST_REDIS_TLS_KEY_FILE" && test -n "$$OWLCRON_TEST_REDIS_TLS_BAD_CERT_FILE" && test -n "$$OWLCRON_TEST_REDIS_TLS_BAD_KEY_FILE" || (echo "Configure the required-client-certificate Redis endpoint and trusted/untrusted client certificates" >&2; exit 1)
	go test -race ./storage -run '^TestRedisReal(TLSConnection|MTLSClientAuthentication)$$' -count=1 -timeout=180s

redis-acl-integration:
	@test -n "$$OWLCRON_TEST_REDIS_ADDR" && test -n "$$OWLCRON_TEST_REDIS_MINIMAL_USERNAME" && test -n "$$OWLCRON_TEST_REDIS_MINIMAL_PASSWORD" && test -n "$$OWLCRON_TEST_REDIS_MINIMAL_PREFIX" && test -n "$$OWLCRON_TEST_REDIS_MINIMAL_DB" && test -n "$$OWLCRON_TEST_REDIS_NOSELECT_USERNAME" && test -n "$$OWLCRON_TEST_REDIS_NOSELECT_PASSWORD" || (echo "Configure the strict ACL and companion no-SELECT users" >&2; exit 1)
	@test -n "$$OWLCRON_TEST_REDIS_USERNAME" && test -n "$$OWLCRON_TEST_REDIS_PASSWORD" || (echo "Configure the isolated fixture observer account for namespace canaries" >&2; exit 1)
	@test -n "$$OWLCRON_TEST_REDIS_TLS_ADDR" && test -n "$$OWLCRON_TEST_REDIS_TLS_CA_FILE" && test -n "$$OWLCRON_TEST_REDIS_TLS_CERT_FILE" && test -n "$$OWLCRON_TEST_REDIS_TLS_KEY_FILE" || (echo "Configure client-certificate TLS for the combined minimal-ACL case" >&2; exit 1)
	go test -race ./storage -run '^TestRedisRealMinimalACL$$' -count=1 -timeout=180s

redis-storage-faults:
	@test -n "$$OWLCRON_TEST_REDIS_ADDR" && test -n "$$OWLCRON_TEST_REDIS_CONTAINER" || (echo "Configure a labeled isolated Redis service and its control identifier" >&2; exit 1)
	go test -race ./storage -run '^TestRedisReal(TransportFaultRecovery|CommittedResponseLossRecovery|ServerSIGKILLRecovery)$$' -count=1 -timeout=240s

# Real daemon processes, SIGKILL, TCP partitions, TLS cluster takeover and recovery.
# Self-contained Filesystem backend; external remote services are not required.
process-integration:
	go test -race . -run '^TestProcess(SIGKILL|Cluster)' -count=1 -timeout=300s

process-integration-repeat:
	go test -race . -run '^TestProcess(SIGKILL|Cluster)' -count=3 -timeout=600s

# Creates only dedicated labelled services; requires Docker and host Python 3.
linux-acceptance:
	bash tests/run-linux-acceptance.sh

remote-storage-faults:
	@test -n "$$OWLCRON_TEST_S3_ENDPOINT" && test -n "$$OWLCRON_TEST_COUCHBASE_CONNECTION" && test -n "$$OWLCRON_TEST_REDIS_ADDR" && test -n "$$OWLCRON_TEST_S3_CONTAINER" && test -n "$$OWLCRON_TEST_COUCHBASE_CONTAINER" && test -n "$$OWLCRON_TEST_REDIS_CONTAINER" || (echo "Configure all three isolated remote services and their control identifiers" >&2; exit 1)
	@test -n "$$OWLCRON_TEST_REDIS_TLS_ADDR" && test -n "$$OWLCRON_TEST_REDIS_TLS_CA_FILE" || (echo "Configure Redis TLS and its trusted CA file" >&2; exit 1)
	go test -race ./storage -run '^Test(S3RealStorage|S3RealTransportFaultRecovery|S3RealCommittedResponseLossRecovery|S3RealServerRestartRecovery|CouchbaseRealStorage|CouchbaseRemoteFaultRecovery|RedisReal.*)$$' -count=1 -timeout=900s

remote-process-integration:
	@test -n "$$OWLCRON_REMOTE_PROCESS_BACKENDS" || (echo "Set OWLCRON_REMOTE_PROCESS_BACKENDS=S3,Couchbase,Redis and matching configuration" >&2; exit 1)
	go test -race . -run '^TestProcessRemoteClusterRecovery$$' -count=1 -timeout=900s

linux-load-acceptance:
	bash tests/run-linux-load-acceptance.sh

cluster-load-acceptance:
	go test -race . -run '^TestProcessClusterLoadedConvergence$$' -count=3 -timeout=600s

capacity-acceptance:
	go test -race . -run '^TestProcessCapacityProfile$$' -count=1 -timeout=600s

# Long duration and outer timeout must be chosen explicitly; never imply 24h ran.
soak-acceptance:
	@test -n "$$OWLCRON_SOAK_DURATION" && test -n "$$OWLCRON_PROCESS_SOAK_DURATION" && test -n "$$OWLCRON_LOAD_SOAK_TIMEOUT" || (echo "Set both soak durations and OWLCRON_LOAD_SOAK_TIMEOUT" >&2; exit 1)
	go test -race . -run '^TestProcess(ClusterSoak|SoakProfile)$$' -count=1 -timeout="$$OWLCRON_LOAD_SOAK_TIMEOUT"
