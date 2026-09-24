.PHONY: proto test run

proto:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/twopc.proto

test:
	go test ./... -v

run:
	go run main.go
