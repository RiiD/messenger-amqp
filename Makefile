export SHELL:=/bin/bash
export SHELLOPTS:=$(if $(SHELLOPTS),$(SHELLOPTS):)pipefail:errexit

.ONESHELL:

test:
	go test ./... -short

test-integration:
	function tearDown {
		docker-compose down
	}
	trap tearDown EXIT
	docker-compose up -d
	docker run -e CGO_ENABLED=0 -it --network messenger-amqp_messenger-amqp-integration-test -w /app -v $(shell pwd):/app golang:1.17.6-alpine3.15 go test ./... -run TestIntegration
