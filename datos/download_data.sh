#!/bin/bash

for h in $(seq 0 23); do
  wget "https://data.gharchive.org/2025-10-01-${h}.json.gz"
done
