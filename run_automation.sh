#! /bin/bash

script_start=`date -u +%s`
./src/automation/run_automation.py --config_path ./src/automation/config_test.json >logs/log_$script_start.log 2>&1 &
disown
echo "Process started and disowned from terminal."
echo "stdout and stderr logged in: logs/log_${script_start}.log"
echo "To view live log type:"
echo "less +F logs/log_${script_start}.log"
