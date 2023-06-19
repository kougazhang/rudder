commit: format test
	git add -A .
	git commit -m "$(info)"
	git push origin master
tag:
	git tag $(tag)
	git push origin $(tag)
format:
	@find ./ -type f -name "*.go" -exec gofmt -w {} \;
test:
	go test ./...