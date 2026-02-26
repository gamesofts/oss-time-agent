#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

if ! command -v mvn >/dev/null 2>&1; then
  echo "[build] ERROR: mvn not found in PATH" >&2
  exit 1
fi

echo "[build] cleanup..."
rm -rf target/

echo "[build] Packaging agent and verify jars..."
mvn -DskipTests package

shopt -s nullglob
agent_candidates=(target/oss-time-agent-*.jar)
verify_candidates=(target/oss-time-agent-*-verify.jar)
shopt -u nullglob

agent_jar=""
verify_jar=""
for f in "${agent_candidates[@]:-}"; do
  case "$f" in
    *-verify.jar|target/original-*.jar) ;;
    *) agent_jar="$f"; break ;;
  esac
done
for f in "${verify_candidates[@]:-}"; do
  verify_jar="$f"
  break
done

if [[ -z "$agent_jar" || ! -f "$agent_jar" ]]; then
  echo "[build] ERROR: agent jar not found under target/" >&2
  exit 1
fi
if [[ -z "$verify_jar" || ! -f "$verify_jar" ]]; then
  echo "[build] ERROR: verify jar not found under target/" >&2
  exit 1
fi

echo

echo "[build] OK"
echo "[build] Agent jar : $agent_jar"
echo "[build] Verify jar: $verify_jar"
echo

echo "Without agent:"
echo "java -jar $verify_jar"
echo

echo "With agent:"
echo "java -javaagent:$agent_jar -jar $verify_jar"
