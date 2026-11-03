.PHONY: all build clean

# compile all tools by default
all: build

build:
	@echo "Building csv2dbf..."
	go build -ldflags="-s -w" -o bin/csv2dbf ./cmd/csv2dbf
	@echo "Building dbf2csv..."
	go build -ldflags="-s -w" -o bin/dbf2csv ./cmd/dbf2csv

clean:
	rm -rf bin/
