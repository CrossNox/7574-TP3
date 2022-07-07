#! /bin/sh

# Little utility to kill a docker container of a particular network randomly
# If passed a second arg, it will constantly try to kill the container, trying to win the race against any kind of reviver

# Use:
#   ./chaos-gorilla.sh <network-name> <optional: forever-flag>
# Examples:
#   ./chaos-gorilla.sh lazarus_net
#   ./chaos-gorilla.sh lazarus_net true

# We can exclude certain containers from being killed, hardcoding them.
EXCLUDE='(rabbitmq|client)'

network=$1

available=$(docker network inspect --format '{{range $v := .Containers}}{{printf "%s  " $v.Name}}{{end}}' $network)
if [ $? -eq 1 ]; then
    exit 0
fi

echo "Available containers in $network:"
echo "$available"

echo ""
echo "Killing a random container in $network"

# Inspect the network | Filter out our exclusions | Trim the last newline | Select one line at random | Stop it
container=$(docker network inspect --format '{{range $v := .Containers}}{{printf "%s\n" $v.Name}}{{end}}' $network | sed --regexp-extended 's/\b$EXCLUDE\b//g' | sed -z '$ s/\n$//' | shuf -n 1 | xargs docker stop --time 1)
echo "$container is DEAD"

if [ $# -eq 2 ]; then
    while [ true ]; do
        if c=$(docker stop filter_columns_averager_2 --time 1 2>/dev/null); then
          echo "$container is DEAD AGAIN"
        fi
        sleep 0.5
    done
fi
