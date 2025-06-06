.PHONY: run-tests
run-tests:
	@go test -v -failfast `go list ./...` -cover

.PHONY: run-tests-report
run-tests-report:
	@go test -v -failfast `go list ./...` -cover -coverprofile=coverage.out -json > test-report.out

.PHONY: run-integ-tests
run-integ-tests:
	@go test -v -failfast `go list ./...` -cover -tags=integration

.PHONY: run-integ-tests-report
run-integ-tests-report:
	@go test -v -failfast `go list ./...` -cover  -tags=integration -coverprofile=coverage.out -json > test-report.out

.PHONY:
mock-install:
	@go install go.uber.org/mock/mockgen@v0.4.0

.PHONY: mock
mock:
	@`go env GOPATH`/bin/mockgen -source ./$(util)/$(subutil).go -destination ./tests/mock/$(util)/$(subutil).go

.PHONY: mock-all
mock-all:
	@make mock util=auth subutil=auth
	@make mock util=configbuilder subutil=configbuilder
	@make mock util=configreader subutil=configreader
	@make mock util=instrument subutil=instrument
	@make mock util=log subutil=log
	@make mock util=parser subutil=parser
	@make mock util=parser subutil=csv
	@make mock util=parser subutil=excel
	@make mock util=parser subutil=json
	@make mock util=query subutil=sql_builder
	@make mock util=sql subutil=sql
	@make mock util=sql subutil=sql_tx
	@make mock util=sql subutil=sql_stmt
	@make mock util=sql subutil=sql_cmd
