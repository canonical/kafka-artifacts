#!/usr/bin/env bash

# the property key printed to stdout, or nothing
function active_key() {
    [[ "${1}" =~ ^[[:space:]]*([^#[:space:]][^=[:space:]]*)[[:space:]]*= ]] && printf '%s' "${BASH_REMATCH[1]}"
}

function assigns_key() {
    [[ "${1}" =~ ^[[:space:]]*#?[[:space:]]*([^#=[:space:]][^=[:space:]]*)[[:space:]]*= ]] && [ "${BASH_REMATCH[1]}" = "${2}" ]
}

# prints properties value from its last active set or "null" when absent
function get_prop() {
    local file="${1}" key="${2}" line value="null"

    [ -f "${file}" ] || { echo "null"; return; }

    while IFS= read -r line || [ -n "${line}" ]; do
        [ "$(active_key "${line}")" = "${key}" ] && value="${line#*=}"
    done < "${file}"

    printf '%s\n' "${value}"
}

# Sets property to value
function set_prop() {
    local file="${1}" key="${2}" value="${3}" line replaced="" tmp
    tmp="$(mktemp)"

    while IFS= read -r line || [ -n "${line}" ]; do
        if [ -z "${replaced}" ] && assigns_key "${line}" "${key}"; then
            printf '%s=%s\n' "${key}" "${value}"
            replaced="yes"
        else
            printf '%s\n' "${line}"
        fi
    done < "${file}" > "${tmp}"

    [ -n "${replaced}" ] || printf '%s=%s\n' "${key}" "${value}" >> "${tmp}"

    # cat back rather than mv, so the destination keeps its owner and mode
    cat "${tmp}" > "${file}"
    rm -f "${tmp}"
}

# removes every assignment of the property key
function remove_prop() {
    local file="${1}" key="${2}" line tmp
    tmp="$(mktemp)"

    while IFS= read -r line || [ -n "${line}" ]; do
        assigns_key "${line}" "${key}" || printf '%s\n' "${line}"
    done < "${file}" > "${tmp}"

    cat "${tmp}" > "${file}"
    rm -f "${tmp}"
}
