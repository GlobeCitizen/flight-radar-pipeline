#!/bin/bash

# Set the Prefect API URL
prefect config set PREFECT_API_URL="http://prefect-server:4200/api"

# Keep the container running
tail -f /dev/null