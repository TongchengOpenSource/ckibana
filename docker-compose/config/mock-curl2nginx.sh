HOST=mock-nginx
DELAY=5
while true; do
  random_num=$((RANDOM))
  curl  "http://${HOST}"
  curl -X POST  "http://${HOST}/mock-post"
  if [ $((random_num % 10)) -eq 0 ]; then
      curl -X DELETE  "http://${HOST}/mock-delete"
  fi
  if [ $((random_num % 10)) -eq 0 ]; then
      curl  "http://${HOST}/mock-500"
  fi
  if [ $((random_num % 10)) -eq 0 ]; then
      curl  "http://${HOST}/400"
  fi
  # sleep $DELAY
done