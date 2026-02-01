#!/bin/sh
output_dir="./outputs"
device_inventory_path="./data/device_inventory.csv"
interface_stats_path="./data/interface_stats.csv"
syslog_path="./data/syslog.jsonl"

python3 pipeline.py --output-dir $output_dir \
                    --device-inventory-path $device_inventory_path \
                    --interface-stats-path $interface_stats_path \
                    --syslog-path $syslog_path