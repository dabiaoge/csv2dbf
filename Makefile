.PHONY: all build clean

# 默认编译所有工具
all: build

build:
	@echo "Building csv2dbf..."
	go build -o bin/csv2dbf ./cmd/csv2dbf
	@echo "Building dbf2csv..."
	go build -o bin/dbf2csv ./cmd/dbf2csv

clean:
	rm -rf bin/