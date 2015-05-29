all: build

build:
	docker build -t convox/tail .

test: build
	@export $(shell cat ../.env); docker run --env-file ../.env convox/tail $${KINESIS_TEST}

vendor:
	godep save -r -copy=true ./...
