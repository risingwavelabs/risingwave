#!/usr/bin/env bash

stop_standalone() {
  pkill standalone
  cargo make ci-kill
}
