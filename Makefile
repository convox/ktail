all: build

build:
	docker build -t convox/ktail .

test: build
	docker run --env-file .env convox/ktail $(STREAM)

vendor:
	godep save -r -copy=true ./...
