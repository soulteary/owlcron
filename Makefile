.PHONY: build test race vet frontend frontend-test integration process-integration process-integration-repeat linux-acceptance remote-storage-faults remote-process-integration linux-load-acceptance capacity-acceptance soak-acceptance cluster-load-acceptance fmt-check licenses-check licenses-update docs-check smoke

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

build:
	go build -o owlcron-engine .
	go build -o owlcron-script ./cmd/owlcron-script
	cd bin/echo-plugin && go build -o echo-plugin .

test:
	go test ./... -count=1 -timeout=360s

race:
	go test -race ./... -count=1 -timeout=360s

vet:
	go vet ./...

frontend:
	cd htdocs-deps && node build.js dist

frontend-test:
	node --test tests/job_log_ui.test.cjs

integration:
	@test -n "$$OWLCRON_TEST_S3_ENDPOINT" || (echo "Set OWLCRON_TEST_S3_ENDPOINT" >&2; exit 1)
	@test -n "$$OWLCRON_TEST_COUCHBASE_CONNECTION" || (echo "Set OWLCRON_TEST_COUCHBASE_CONNECTION" >&2; exit 1)
	go test -race ./... -count=1 -timeout=240s

# Real daemon processes, SIGKILL, TCP partitions, TLS cluster takeover and recovery.
# Self-contained Filesystem backend; external S3/Couchbase services are not required.
process-integration:
	go test -race . -run '^TestProcess(SIGKILL|Cluster)' -count=1 -timeout=300s

process-integration-repeat:
	go test -race . -run '^TestProcess(SIGKILL|Cluster)' -count=3 -timeout=600s

# Creates only dedicated labelled services; requires Docker and host Python 3.
linux-acceptance:
	bash tests/run-linux-acceptance.sh

remote-storage-faults:
	@test -n "$$OWLCRON_TEST_S3_ENDPOINT" && test -n "$$OWLCRON_TEST_COUCHBASE_CONNECTION" && test -n "$$OWLCRON_TEST_S3_CONTAINER" && test -n "$$OWLCRON_TEST_COUCHBASE_CONTAINER" || (echo "Configure both isolated remote services and their control identifiers" >&2; exit 1)
	go test -race ./storage -run '^Test(S3RealStorage|S3RealTransportFaultRecovery|S3RealCommittedResponseLossRecovery|S3RealServerRestartRecovery|CouchbaseRealStorage|CouchbaseRemoteFaultRecovery)$$' -count=1 -timeout=600s

remote-process-integration:
	@test -n "$$OWLCRON_REMOTE_PROCESS_BACKENDS" || (echo "Set OWLCRON_REMOTE_PROCESS_BACKENDS=S3,Couchbase and matching configuration" >&2; exit 1)
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
