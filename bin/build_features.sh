#!/bin/bash
echo "Starting Feature Process"

feature_file=$(dirname $(dirname "$(readlink -f $0)"))/src/features.py;

python3 $dwnld_file sec
python3 $dwnld_file stocks

echo "Finished Running Feature"
