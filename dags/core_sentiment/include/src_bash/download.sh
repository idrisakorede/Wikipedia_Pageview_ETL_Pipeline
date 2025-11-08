#!/bin/bash
set -e

# Define output directory
output_path="${RAW_PAGEVIEWS_DIR}"
mkdir -p "$output_path"

# Define Wikimedia dump URL
page_url="https://dumps.wikimedia.org/other/pageviews/2025/2025-10/"

# Get HTML and pick a random gzip file
html=$(curl -s "$page_url")
link=$(echo "$html" | grep -oiP 'href="\K[^"]+\.gz' | sort -u | shuf -n 1)
download_link="${page_url}${link}"

# File name
zipped_file=$(basename "$link")

echo "Downloading: $download_link"
curl -o "$output_path/$zipped_file" "$download_link"

echo "Processing: $zipped_file"