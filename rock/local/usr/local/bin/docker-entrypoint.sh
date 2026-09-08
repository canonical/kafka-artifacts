#!/usr/bin/env bash
#
# Ported from the official Apache Kafka image, which is the configuration
# interface anything written against apache/kafka expects: KAFKA_* in the
# environment, translated into server.properties by
# kafka.docker.KafkaDockerWrapper, and CLUSTER_ID formatting the KRaft storage
# on the way up.
#
# Combines the upstream run, configureDefaults, configure and (jvm) launch
# scripts into one entrypoint, dropping the CDS .jsa startup optimisation.
#
# https://github.com/apache/kafka/tree/trunk/docker/resources/common-scripts
# https://github.com/apache/kafka/blob/trunk/docker/jvm/launch

set -o nounset -o errexit

# Trace may expose passwords/credentials by printing them, so turn on with care.
if [ "${TRACE:-}" = "true" ]; then
  set -o verbose -o xtrace
fi

# Each argument is a KAFKA_* override, exactly as "docker run <image> KAFKA_FOO=bar".
if [ "$#" -ne 0 ]; then
  echo "===> Overriding env params with args ..."
  for var in "$@"; do
    export "${var?}"
  done
fi

# A random-uuid cluster id by default, so "docker run <image>" formats and
# starts a working single node without one having to be supplied.
if [ -z "${CLUSTER_ID:-}" ]; then
  CLUSTER_ID="$("${KAFKA_HOME}/bin/kafka-storage.sh" random-uuid | tail -n1 | tr -d '[:space:]')"
  echo "===> CLUSTER_ID not set, generated ${CLUSTER_ID}"
fi
export CLUSTER_ID

# A controller-only node advertises nothing to clients; anything else derives
# LISTENERS from ADVERTISED_LISTENERS when only the latter is given, so the
# process still binds every advertised port.
if [ "${KAFKA_PROCESS_ROLES:-}" = "controller" ]; then
  unset KAFKA_ADVERTISED_LISTENERS
elif [ -z "${KAFKA_LISTENERS:-}" ] && [ -n "${KAFKA_ADVERTISED_LISTENERS:-}" ]; then
  KAFKA_LISTENERS="$(echo "${KAFKA_ADVERTISED_LISTENERS}" | sed -e 's|://[^:]*:|://0.0.0.0:|g')"
  export KAFKA_LISTENERS
fi

echo "===> Using cluster id ${CLUSTER_ID} ..."

# KafkaDockerWrapper reads the template and log4j configs from
# --default-configs-dir, folds in anything under --mounted-configs-dir and the
# KAFKA_* environment, writes the result to --final-configs-dir and formats the
# KRaft storage with CLUSTER_ID. "already formatted" on a reused volume is fine.
if ! result="$("${KAFKA_HOME}/bin/kafka-run-class.sh" kafka.docker.KafkaDockerWrapper setup \
    --default-configs-dir /etc/kafka/docker \
    --mounted-configs-dir /mnt/shared/config \
    --final-configs-dir "${KAFKA_HOME}/config" 2>&1)"; then
  echo "${result}"
  echo "${result}" | grep -iq "already formatted" || exit 1
fi
echo "${result}"

exec "${KAFKA_HOME}/bin/kafka-server-start.sh" "${KAFKA_HOME}/config/server.properties"
