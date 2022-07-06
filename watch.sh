rm -rf /tmp/docker_watch_logs
mkdir /tmp/docker_watch_logs
for c in $(docker ps -a --format="{{.Names}}"); do
	if [[ $c == "rabbbitmq" ]]; then
		continue
	fi
	if [[ $c == "coordinator" ]]; then
		continue
	fi
	if [[ $c == "client" ]]; then
		continue
	fi
	docker logs -f $c >/tmp/docker_watch_logs/$c.log 2>/tmp/docker_watch_logs/$c.err &
done
tail -f /tmp/docker_watch_logs/*.{log,err}
