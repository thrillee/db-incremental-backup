# General targets

BINARY_NAME=dbHelper

build: 
	GOARCH=amd64 GOOS=darwin go build -o ${BINARY_NAME}-darwin main.go
	GOARCH=amd64 GOOS=linux go build -o ${BINARY_NAME}-linux main.go
	GOARCH=amd64 GOOS=windows go build -o ${BINARY_NAME}-windows main.go

clean:
	rm -rf $(BIN_NAME)

# Define variables

# Specify the shell to use for make commands
SHELL := /bin/sh
