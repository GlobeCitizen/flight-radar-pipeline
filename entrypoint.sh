#!/bin/bash

# entrypoint.sh

if [ "$1" = "main" ]; then
    python main.py
elif [ "$1" = "answers" ]; then
    python answers.py
else
    tail -f /dev/null
fi