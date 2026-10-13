#!/usr/bin/env bash
#
# pebble readiness probe: TCP-connect to the broker's client listener port(s),
# derived from the rendered server.properties, so the check follows whatever
# port the broker binds (including a TLS-only listener). A TCP connect succeeds
# against a TLS port because the socket accepts before any TLS negotiation

set -o nounset -o errexit -o pipefail

config=/opt/kafka/config/server.properties

# listeners and controller listener names, as rendered for this node
listeners="$(grep -E '^listeners=' "${config}" | tail -n1 | cut -d= -f2-)"
controller_names="$(grep -E '^controller\.listener\.names=' "${config}" | tail -n1 | cut -d= -f2-)"

IFS=',' read -ra controller_name_list <<< "${controller_names}"

is_controller_name() {
  local name="$1" candidate
  for candidate in "${controller_name_list[@]}"; do
    [ "${name}" = "${candidate}" ] && return 0
  done
  return 1
}

client_ports=()
controller_ports=()
IFS=',' read -ra entries <<< "${listeners}"
for entry in "${entries[@]}"; do
  # each entry looks like NAME://host:port
  name="${entry%%://*}"
  port="${entry##*:}"
  [ -z "${port}" ] && continue
  if is_controller_name "${name}"; then
    controller_ports+=("${port}")
  else
    client_ports+=("${port}")
  fi
done

# controller-only nodes expose no client listener, so fall back to the controller port
probe_ports=("${client_ports[@]}")
[ "${#probe_ports[@]}" -eq 0 ] && probe_ports=("${controller_ports[@]}")

if [ "${#probe_ports[@]}" -eq 0 ]; then
  echo "no listener ports found in ${config}" >&2
  exit 1
fi

for port in "${probe_ports[@]}"; do
  if ! timeout 2 bash -c "exec 3<>/dev/tcp/127.0.0.1/${port}" 2>/dev/null; then
    echo "port ${port} not accepting connections" >&2
    exit 1
  fi
done
