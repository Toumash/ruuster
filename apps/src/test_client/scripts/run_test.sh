#!/usr/bin/env bash

RUUSTER_HOME_DIR="$HOME/rust-ws/ruuster"
RUUSTER_BUILD_DIR="$RUUSTER_HOME_DIR/target/debug"

cd $RUUSTER_HOME_DIR || exit
RUST_LOG=info $RUUSTER_BUILD_DIR/run_scenario  --server-addr "http://127.0.0.1:50051" \
  --config-file "$RUUSTER_HOME_DIR/apps/src/test_client/scenarios/all_consumers.json" \
  --builder-bin "$RUUSTER_BUILD_DIR/scenario_builder" \
  --consumer-bin "$RUUSTER_BUILD_DIR/consumer" \
  --producer-bin "$RUUSTER_BUILD_DIR/producer"
