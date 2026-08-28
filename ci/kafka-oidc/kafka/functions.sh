#!/usr/bin/env bash

wait_for_url() {
    local url="$1"
    local message="$2"
    local max_attempts="${3:-60}"
    local attempt
    local cmd

    if [[ "$url" == https* ]]; then
        cmd=(curl -k -sL --max-time 2 -o /dev/null -w "%{http_code}" "$url")
    else
        cmd=(curl -sL --max-time 2 -o /dev/null -w "%{http_code}" "$url")
    fi

    for ((attempt = 1; attempt <= max_attempts; attempt++)); do
        if [[ "$("${cmd[@]}")" == "200" ]]; then
            return 0
        fi
        echo "$message ($url, attempt ${attempt}/${max_attempts})"
        if ((attempt < max_attempts)); then
            sleep 2
        fi
    done

    echo "Timed out: $message ($url)" 1>&2
    return 1
}
