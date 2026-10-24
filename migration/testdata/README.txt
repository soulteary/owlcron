Synthetic fixtures only. No production data or credentials.
The checker never executes the sample plugin commands or script strings.

cronicle-export.txt follows Cronicle 0.9.135 bin/storage-cli.js export_data:
https://github.com/jhuckaby/Cronicle/blob/master/bin/storage-cli.js
The exporter emits a v1.0 header, KEY - JSON lines, standalone users/<name>
records, and headers/pages for seven global lists. These records were authored
for testing and were not copied from an installation.

Plugin stdin/stdout contract fixtures are in plugin/testdata/cronicle-job.json
and plugin/testdata/cronicle-output.jsonl. They model the JSON-line contract
already tested by plugin/protocol_test.go, including numeric complete:1,
fractional progress, ordinary logs and output following completion.

List pages, including negative indices after listUnshift, follow:
https://github.com/jhuckaby/pixl-server-storage/blob/master/list.js
