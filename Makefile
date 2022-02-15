GO_BIN_FILES=cmd/github/github.go cmd/encrypt/encrypt.go
#for race CGO_ENABLED=1
# GO_ENV=GOOS=linux CGO_ENABLED=1
GO_ENV=GOOS=linux CGO_ENABLED=0
# GO_BUILD=go build -ldflags '-s -w' -race
GO_BUILD=go build -ldflags '-s -w'
GO_FMT=gofmt -s -w
GO_LINT=golint -set_exit_status
GO_VET=go vet
GO_IMPORTS=goimports -w
GO_ERRCHECK=errcheck -asserts -ignore '[FS]?[Pp]rint*'
BINARIES=github encrypt
all: check ${BINARIES}
github: cmd/github/github.go
	 ${GO_ENV} ${GO_BUILD} -o github cmd/github/github.go
encrypt: cmd/encrypt/encrypt.go
	 ${GO_ENV} ${GO_BUILD} -o encrypt cmd/encrypt/encrypt.go
fmt: ${GO_BIN_FILES}
	${GO_FMT} ${GO_BIN_FILES}
lint: ${GO_BIN_FILES}
	${GO_LINT} ${GO_BIN_FILES}
vet: ${GO_BIN_FILES}
	${GO_VET} cmd/github/github.go
	${GO_VET} cmd/encrypt/encrypt.go
imports: ${GO_BIN_FILES}
	${GO_IMPORTS} ${GO_BIN_FILES}
errcheck: ${GO_BIN_FILES}
	${GO_ERRCHECK} cmd/github/github.go
	${GO_ERRCHECK} cmd/encrypt/encrypt.go
check: fmt lint imports vet errcheck
clean:
	rm -rf ${BINARIES}
.PHONY: all
