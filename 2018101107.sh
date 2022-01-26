#!/bin/bash
for dir in "$@"
do
    python3 main.py "$dir"
done
