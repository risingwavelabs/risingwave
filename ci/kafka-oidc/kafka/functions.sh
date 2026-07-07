#!/usr/bin/env bash

wait_for_url() {
    local url="$1"
    local message="$2"
    local cmd

    if [[ "$url" == https* ]]; then
        cmd=(curl -k -sL -o /dev/null -w "%{http_code}" "$url")
    else
        cmd=(curl -sL -o /dev/null -w "%{http_code}" "$url")
    fi

    until [[ "$("${cmd[@]}")" == "200" ]]; do
        echo "$message ($url)"
        sleep 2
    done
}
