#!/usr/bin/env bash
set -Eeuo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
OUT="$(mktemp -d)"
trap 'rm -rf "$OUT"' EXIT

find audit-service/src/main/java -name '*.java' -print0 \
  | xargs -0 javac --release 21 -Xlint:all -d "$OUT"
javac --release 21 -Xlint:all -cp "$OUT" -d "$OUT" \
  audit-service/src/test/java/org/g12/audit/AuditProtocolSelfTest.java

# The Gateway ACK reader depends only on the JDK; compile the real production
# reader and its protocol/model classes to test it against Audit's real writer.
javac --release 21 -Xlint:all -cp "$OUT" -d "$OUT" \
  src/main/java/org/java/AckRingProtocol.java \
  src/main/java/org/java/AuditAcknowledgement.java \
  src/main/java/org/java/MappedAckRingReader.java \
  integration-tests/java/org/g12/audit/AuditAckInteropSelfTest.java

java -cp "$OUT" org.g12.audit.AuditProtocolSelfTest
java -cp "$OUT" org.g12.audit.AuditAckInteropSelfTest
