Run Jaeger...

  docker run -d --name jaeger   -e COLLECTOR_ZIPKIN_HTTP_PORT=9411   -p 5775:5775/udp   -p 6831:6831/udp   -p 6832:6832/udp   -p 5778:5778   -p 16686:16686   -p 14268:14268   -p 9411:9411   jaegertracing/all-in-one:1.12

Pipe the task...

  bosh task --debug 1234 | go run ./cmd/boshtaskdebugjaeger

Open the URL it prints (something like http://localhost:16686/trace/1cfa67194cc4d8ef).
