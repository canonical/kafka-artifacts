#!/usr/bin/env bash
# maps snap options onto server.properties
# manual changes supercede `snap set`
# diffs the current value against the most recent or shipped default

source "${SNAP}/opt/shared/bin/set-conf.sh"

CONF_FILE="${SNAP_COMMON}/etc/kafka/server.properties"

# default baseline file set during install hook
# falls back to the upstream file
DEFAULT_CONF_FILE="${SNAP_COMMON}/ops/defaults.properties"
if [ ! -f "${DEFAULT_CONF_FILE}" ]; then
    DEFAULT_CONF_FILE="${SNAP}/opt/kafka/config/server.properties"
fi
RENDERED_STATE_FILE="${SNAP_COMMON}/ops/rendered-config.properties"
LOG_FILE="${SNAP_COMMON}/ops/snap/logs/hook-configure.log"

# key ("." rewritten to "-") as "." not permitted in `snap set`
declare -A CONFIG_OPTIONS=(
    [node-id]="node.id"
    [process-roles]="process.roles"
    [listeners]="listeners"
    [advertised-listeners]="advertised.listeners"
    [controller-quorum-voters]="controller.quorum.voters"
    [controller-listener-names]="controller.listener.names"
    [inter-broker-listener-name]="inter.broker.listener.name"
    [listener-security-protocol-map]="listener.security.protocol.map"
    [log-dirs]="log.dirs"
    [num-partitions]="num.partitions"
    [default-replication-factor]="default.replication.factor"
    [offsets-topic-replication-factor]="offsets.topic.replication.factor"
    [transaction-state-log-replication-factor]="transaction.state.log.replication.factor"
    [transaction-state-log-min-isr]="transaction.state.log.min.isr"
    [auto-create-topics-enable]="auto.create.topics.enable"
)

# write to both the log and stdout, since stdout only survives when the hook fails.
function report() {
    local message="${1}"
    local log_dir
    log_dir="$(dirname "${LOG_FILE}")"

    [ -d "${log_dir}" ] || mkdir -p "${log_dir}"

    echo "${message}"
    echo "$(date '+%F %T') ${message}" >> "${LOG_FILE}"
}

function supported_options() {
    printf '%s\n' "${!CONFIG_OPTIONS[@]}" | sort
}

function options_that_are_set() {
    local option value
    for option in "${@}"; do
        if value="$(snapctl get "${option}" 2>/dev/null)" && [ -n "${value}" ]; then
            echo "${option}"
        fi
    done
}

function assigned_options() {
    options_that_are_set "${!CONFIG_OPTIONS[@]}"
}

function candidate_options() {
    sed -n -E 's/^[[:space:]]*#?[[:space:]]*([A-Za-z0-9._-]+)[[:space:]]*=.*/\1/p' \
        "${DEFAULT_CONF_FILE}" | tr '.' '-' | sort -u
}

function ensure_state_file() {
    local state_dir
    state_dir="$(dirname "${RENDERED_STATE_FILE}")"

    [ -d "${state_dir}" ] || mkdir -p "${state_dir}"
    [ -f "${RENDERED_STATE_FILE}" ] || : > "${RENDERED_STATE_FILE}"
}

# fails the hook for non-permitted server.properties keys
function reject_unsupported_options() {
    local option
    local -a candidates=()
    local -a unsupported=()

    while read -r option; do
        if [ -z "${CONFIG_OPTIONS[${option}]+isset}" ]; then
            candidates+=("${option}")
        fi
    done < <(candidate_options)

    while read -r option; do
        [ -n "${option}" ] && unsupported+=("${option}")
    done < <(options_that_are_set "${candidates[@]}")

    if [ ${#unsupported[@]} -gt 0 ]; then
        report "option(s) that cannot be applied: ${unsupported[*]}"
        report "set the matching key in ${CONF_FILE} instead"
        report "options settable through 'snap set':"
        while read -r option; do
            report "  ${option}"
        done < <(supported_options)
        return 1
    fi
}

# what server.properties holds as long as nobody edited the key manually
function unmodified_value() {
    local option="${1}"
    local key="${2}"
    local rendered

    rendered="$(get_prop "${RENDERED_STATE_FILE}" "${option}")"
    if [ "${rendered}" != "null" ]; then
        echo "${rendered}"
    else
        get_prop "${DEFAULT_CONF_FILE}" "${key}"
    fi
}

function is_hand_edited() {
    local option="${1}"
    local key="${2}"

    [ "$(get_prop "${CONF_FILE}" "${key}")" \
        != "$(unmodified_value "${option}" "${key}")" ]
}

# refuses and rolls back a `snap set` whose key is manually editted
# runs before any write
function reject_blocked_options() {
    local option key value message
    local -a blocked=()

    while read -r option; do
        [ -n "${option}" ] || continue

        key="${CONFIG_OPTIONS[${option}]}"
        value="$(snapctl get "${option}")"

        if is_hand_edited "${option}" "${key}" \
            && [ "${value}" != "$(get_prop "${RENDERED_STATE_FILE}" "${option}")" ]; then
            blocked+=("cannot apply the '${option}' option: ${key} is set to '$(get_prop "${CONF_FILE}" "${key}")' in server.properties")
        fi
    done < <(assigned_options)

    if [ ${#blocked[@]} -gt 0 ]; then
        for message in "${blocked[@]}"; do
            report "${message}"
        done
        report "a value edited by hand takes precedence; to hand the key back to 'snap set', remove that edit from ${CONF_FILE}"
        return 1
    fi
}

function render_option() {
    local option="${1}"
    local value="${2}"
    local key="${CONFIG_OPTIONS[${option}]}"
    local current

    current="$(get_prop "${CONF_FILE}" "${key}")"

    if is_hand_edited "${option}" "${key}"; then
        report "${key} is set to '${current}' in server.properties, ignoring the '${option}' option"
        return
    fi

    if [ "${current}" != "${value}" ]; then
        report "setting ${key} to '${value}'"
        set_prop "${CONF_FILE}" "${key}" "${value}"
        RESTART_REQUIRED="yes"
    fi

    set_prop "${RENDERED_STATE_FILE}" "${option}" "$(get_prop "${CONF_FILE}" "${key}")"
}

# restores the default when an option is unset
function revert_option() {
    local option="${1}"
    local key="${CONFIG_OPTIONS[${option}]}"
    local rendered current default

    rendered="$(get_prop "${RENDERED_STATE_FILE}" "${option}")"
    if [ "${rendered}" == "null" ]; then
        return
    fi

    current="$(get_prop "${CONF_FILE}" "${key}")"
    if [ "${current}" == "${rendered}" ]; then
        default="$(get_prop "${DEFAULT_CONF_FILE}" "${key}")"
        report "'${option}' is unset, restoring ${key} to '${default}'"
        if [ "${default}" == "null" ]; then
            remove_prop "${CONF_FILE}" "${key}"
        else
            set_prop "${CONF_FILE}" "${key}" "${default}"
        fi
        RESTART_REQUIRED="yes"
    fi

    remove_prop "${RENDERED_STATE_FILE}" "${option}"
}

function render_config() {
    local option value

    RESTART_REQUIRED="no"

    for option in $(supported_options); do
        if value="$(snapctl get "${option}" 2>/dev/null)" && [ -n "${value}" ]; then
            render_option "${option}" "${value}"
        else
            revert_option "${option}"
        fi
    done

    if [ "${RESTART_REQUIRED}" == "yes" ]; then
        report "run 'snap restart kafka.server' to apply the new configuration"
    fi
}
