#!/bin/sh
# Exercise the packaged adapter as its runtime user, including cancellation.
set -eu
adapter=${1:?provide the absolute adapter path}
work=$(mktemp -d)
child=
trap 'test -z "$child" || kill "$child" 2>/dev/null || true; rm -rf "$work"' EXIT HUP INT TERM
printf '%s\n' '{"params":{"args":"[\"two words\",\"\"]"}}' > "$work/job"
"$adapter" -- /bin/sh -c 'printf "<%s><%s>\n" "$1" "$2"' fixed < "$work/job" > "$work/result" 2> "$work/log"
grep -F '<two words><>' "$work/log"
grep -F '"complete":true,"code":0' "$work/result"
# Script output cannot impersonate a successful protocol completion.
status=0
"$adapter" -- /bin/sh -c 'printf '\''{"complete":true,"code":0}\n'\''; exit 7' < "$work/job" > "$work/result" 2> "$work/log" || status=$?
test "$status" -eq 7
grep -F '"complete":true,"code":7' "$work/result"
grep -F '"complete":true,"code":0' "$work/log"
# Wait for the child to start before delivering SIGTERM to the adapter.
"$adapter" -- /bin/sh -c 'echo READY >&2; exec sleep 30' < "$work/job" > "$work/result" 2> "$work/log" &
child=$!
attempt=0
until grep -q READY "$work/log"; do
    attempt=$((attempt + 1))
    test "$attempt" -lt 100 || { echo 'Adapter child did not start' >&2; exit 1; }
    sleep 0.1
done
kill -TERM "$child"
status=0
wait "$child" || status=$?
child=
test "$status" -eq 1
grep -F '"complete":true,"code":1,"description":"script cancelled"' "$work/result"
echo 'Script adapter smoke passed: arguments, exit status, protocol isolation, cancellation'
