package main

import (
	"bufio"
	"encoding/binary"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

// Global configuration variables
var (
	flagDelimiter string
	flagQuote     string
	flagNewline   string
	flagEncoding  string
	flagProgress  int // [New] Control progress reporting interval
)

// Constants for program info and FoxPro 2.5 limits
const (
	AppVersion = "1.8.2"
	AppAuthor  = "dabiaoge"

	// FoxPro 2.5 specification constants
	FoxProVersionNoMemo = 0x03     // FoxPro 2.x table without memo file
	MaxFoxProFields     = 255      // Maximum number of fields allowed in FoxPro 2.5
	MaxFoxProRecordLen  = 65504    // Maximum record length in FoxPro 2.5
	MaxCharFieldLen     = 254      // Maximum character field width in FoxPro 2.5
)

// DBFHeader represents the FoxPro 2.5 file header structure (32 bytes)
type DBFHeader struct {
	Version   byte     // 0-0: 0x03 for FoxPro 2.x without memo
	Year      byte     // 1-1: Year - 1900
	Month     byte     // 2-2: Month (1-12)
	Day       byte     // 3-3: Day (1-31)
	NumRecs   uint32   // 4-7: Total number of records in table
	HeaderLen uint16   // 8-9: Header size in bytes (32 + 32*n + 1)
	RecLen    uint16   // 10-11: Length of each record in bytes (including delete flag)
	Reserved1 [16]byte // 12-27: Reserved bytes in FoxPro 2.x
	TableFlag byte     // 28-28: Table flags (0x01: has CDX, 0x00: standard)
	CodePage  byte     // 29-29: Code page mark / language driver ID
	Reserved2 [2]byte  // 30-31: Reserved
}

// DBFField represents the FoxPro 2.5 field descriptor structure (32 bytes)
type DBFField struct {
	Name      [11]byte // 0-10: Field name (max 10 chars, null-terminated)
	Type      byte     // 11-11: Field type ('C', 'N', 'L', 'D', 'M', etc.)
	Reserved1 [4]byte  // 12-15: Field data address in memory (reserved in file)
	Len       byte     // 16-16: Field length (binary representation)
	Dec       byte     // 17-17: Decimal count
	Reserved2 [14]byte // 18-31: Reserved for FoxPro / dBASE
}

// FieldInfo holds internal metadata for a column
type FieldInfo struct {
	Name   string
	Type   byte
	Length int
	Dec    int
}

func init() {
	// Define command line flags
	flag.StringVar(&flagDelimiter, "f", ",", "Field delimiter (single char)")
	flag.StringVar(&flagQuote, "q", "\"", "Quote character")
	flag.StringVar(&flagNewline, "l", "\n", "Line ending (e.g. \"\\n\", \"\\r\\n\")")
	flag.StringVar(&flagEncoding, "e", "GBK", "Encoding (GBK, GB18030, UTF-8)")
	flag.IntVar(&flagProgress, "n", 0, "Show progress every N rows (default 0, disable output)")

	// Custom usage message
	flag.Usage = func() {
		fmt.Printf("CSV2DBF Converter\n")
		fmt.Printf("Version: %s\n", AppVersion)
		fmt.Printf("Author : %s\n\n", AppAuthor)
		fmt.Printf("Usage: %s [options] <csv_file1> [csv_file2] ...\n\n", os.Args[0])
		fmt.Println("Options:")
		flag.PrintDefaults()
		fmt.Println("\nExamples:")
		fmt.Printf("  %s data.csv\n", os.Args[0])
		fmt.Printf("  %s -e GBK -n 5000 data.csv\n", os.Args[0])
		fmt.Printf("  %s -f '|' data.csv\n", os.Args[0])
	}
}

func main() {
	flag.Parse()
	args := flag.Args()

	// Show help if no files provided
	if len(args) < 1 {
		flag.Usage()
		os.Exit(0)
	}

	// Parse escaped characters in flags
	delimiter := parseEscapedChar(flagDelimiter)
	if delimiter == 0 {
		fmt.Fprintf(os.Stderr, "Error: Invalid delimiter '%s'\n", flagDelimiter)
		os.Exit(1)
	}

	quote := parseEscapedChar(flagQuote)

	// Determine encoding
	enc := getEncoding(flagEncoding)
	if enc == nil {
		fmt.Fprintf(os.Stderr, "Error: Unsupported encoding '%s'\n", flagEncoding)
		os.Exit(1)
	}

	for _, csvFile := range args {
		if _, err := os.Stat(csvFile); os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "Error: File not found [%s]\n", csvFile)
			continue
		}

		fmt.Printf("Processing: %s\n", csvFile)
		startTime := time.Now()

		err := convertCSVtoDBF(csvFile, delimiter, quote, enc)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed [%s]: %v\n", csvFile, err)
			continue
		}

		elapsed := time.Since(startTime)
		// [Refactor] Changed time format to seconds with 3 decimal places
		fmt.Printf("Done: %s (Time: %.3fs)\n", csvFile, elapsed.Seconds())
	}
}

func parseEscapedChar(s string) rune {
	if len(s) == 0 {
		return 0
	}
	if len(s) >= 2 && s[0] == '\\' {
		switch s[1] {
		case 'n':
			return '\n'
		case 'r':
			return '\r'
		case 't':
			return '\t'
		case '\\':
			return '\\'
		case '"':
			return '"'
		case '\'':
			return '\''
		case '0':
			return 0
		}
	}
	r, _ := utf8.DecodeRuneInString(s)
	if r == utf8.RuneError {
		return 0
	}
	return r
}

func getEncoding(name string) encoding.Encoding {
	name = strings.ToLower(strings.TrimSpace(name))
	switch name {
	case "utf-8", "utf8":
		return unicode.UTF8
	case "gbk", "gb2312", "gb18030":
		return simplifiedchinese.GB18030
	default:
		return nil
	}
}

// getFoxProCodePageId maps user-specified encoding name to FoxPro 2.5 Code Page Identifier
func getFoxProCodePageId(encName string) byte {
	encName = strings.ToLower(strings.TrimSpace(encName))
	switch encName {
	case "gbk", "gb2312", "gb18030":
		return 0x7A // Simplified Chinese Windows (CP936)
	default:
		return 0x01 // US MS-DOS (CP437) or default OEM
	}
}

func convertCSVtoDBF(csvPath string, comma rune, quote rune, enc encoding.Encoding) error {
	// --- Pass 1: Analyze Structure ---
	fmt.Println("  [1/2] Analyzing field structure...")
	fields, recordCount, err := analyzeCSV(csvPath, comma, quote, enc)
	if err != nil {
		return err
	}
	fmt.Printf("  >> Fields: %d, Records: %d\n", len(fields), recordCount)

	if len(fields) == 0 {
		return fmt.Errorf("no fields found in CSV")
	}

	// Validate FoxPro 2.5 column count limit
	if len(fields) > MaxFoxProFields {
		return fmt.Errorf("field count (%d) exceeds FoxPro 2.5 maximum limit of %d", len(fields), MaxFoxProFields)
	}

	// --- Prepare DBF File ---
	dbfPath := strings.TrimSuffix(csvPath, filepath.Ext(csvPath)) + ".dbf"
	dbfFile, err := os.Create(dbfPath)
	if err != nil {
		return fmt.Errorf("failed to create DBF: %w", err)
	}
	defer dbfFile.Close()

	writer := bufio.NewWriterSize(dbfFile, 4*1024*1024)

	// --- Write Header ---
	if err := writeDBFHeader(writer, fields, recordCount, enc); err != nil {
		return err
	}

	// --- Pass 2: Write Data ---
	fmt.Println("  [2/2] Writing records...")
	if err := writeDBFRecords(csvPath, writer, fields, recordCount, comma, quote, enc); err != nil {
		return err
	}

	// Write FoxPro EOF marker (0x1A)
	if err := writer.WriteByte(0x1A); err != nil {
		return err
	}

	return writer.Flush()
}

// getCSVReader creates a standard CSV reader
func getCSVReader(f *os.File, comma rune, quote rune, enc encoding.Encoding) *csv.Reader {
	// 1. Create a transforming reader that decodes input to UTF-8
	decoder := enc.NewDecoder()
	reader := transform.NewReader(f, decoder)

	// 2. Create CSV reader
	csvReader := csv.NewReader(reader)
	csvReader.Comma = comma

	csvReader.FieldsPerRecord = -1
	csvReader.LazyQuotes = true
	csvReader.TrimLeadingSpace = false
	return csvReader
}

func analyzeCSV(filename string, comma rune, quote rune, enc encoding.Encoding) ([]FieldInfo, uint32, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	r := getCSVReader(f, comma, quote, enc)

	headers, err := r.Read()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read header: %v", err)
	}

	fields := make([]FieldInfo, len(headers))
	for i, name := range headers {
		fields[i] = FieldInfo{
			Name:   strings.ToUpper(strings.TrimSpace(name)),
			Type:   'C',
			Length: 1,
			Dec:    0,
		}
	}

	encoder := enc.NewEncoder()
	var count uint32

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("    Warning: skipping malformed line at record %d: %v\n", count+1, err)
			continue
		}

		for i, val := range record {
			if i >= len(fields) {
				break
			}
			// DBF length is byte length in target encoding
			encodedVal, _, _ := transform.Bytes(encoder, []byte(val))
			l := len(encodedVal)
			if l > fields[i].Length {
				fields[i].Length = l
			}
		}
		count++
	}

	// Restrict length to FoxPro 2.5 maximum Character field width (254 bytes)
	for i := range fields {
		if fields[i].Length > MaxCharFieldLen {
			fields[i].Length = MaxCharFieldLen
		}
	}

	return fields, count, nil
}

func safeTruncateName(name string, enc encoding.Encoding) [11]byte {
	var res [11]byte
	encoder := enc.NewEncoder()
	b, _, _ := transform.Bytes(encoder, []byte(name))

	// In FoxPro 2.5, field names cannot exceed 10 characters and must end with 0x00
	if len(b) > 10 {
		b = b[:10]
	}
	copy(res[:], b)
	return res
}

func writeDBFHeader(w *bufio.Writer, fields []FieldInfo, numRecs uint32, enc encoding.Encoding) error {
	now := time.Now()
	recLen := uint16(1) // 1 byte for delete marker
	for _, f := range fields {
		recLen += uint16(f.Length)
	}

	// Validate FoxPro 2.5 record length limit
	if int(recLen) > MaxFoxProRecordLen {
		return fmt.Errorf("total record length (%d bytes) exceeds FoxPro 2.5 limit (%d bytes)", recLen, MaxFoxProRecordLen)
	}

	headerLen := uint16(32 + 32*len(fields) + 1)

	// Populate standard FoxPro 2.5 DBF Header
	h := DBFHeader{
		Version:   FoxProVersionNoMemo,
		Year:      byte(now.Year() - 1900),
		Month:     byte(now.Month()),
		Day:       byte(now.Day()),
		NumRecs:   numRecs,
		HeaderLen: headerLen,
		RecLen:    recLen,
		TableFlag: 0x00, // 0x00: Standard FoxPro 2.5 table without structural CDX
		CodePage:  getFoxProCodePageId(flagEncoding),
	}

	if err := binary.Write(w, binary.LittleEndian, &h); err != nil {
		return err
	}

	for _, f := range fields {
		df := DBFField{
			Name: safeTruncateName(f.Name, enc),
			Type: f.Type,
			Len:  byte(f.Length),
			Dec:  0,
		}
		if err := binary.Write(w, binary.LittleEndian, &df); err != nil {
			return err
		}
	}

	// Write FoxPro header terminator byte
	return w.WriteByte(0x0D)
}

func writeDBFRecords(csvPath string, w *bufio.Writer, fields []FieldInfo, total uint32, comma rune, quote rune, enc encoding.Encoding) error {
	f, err := os.Open(csvPath)
	if err != nil {
		return err
	}
	defer f.Close()

	r := getCSVReader(f, comma, quote, enc)
	if _, err := r.Read(); err != nil {
		return err
	}

	encoder := enc.NewEncoder()

	recordSize := 1
	for _, f := range fields {
		recordSize += f.Length
	}
	recordBuf := make([]byte, recordSize)

	var processed uint32

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		fillSpace(recordBuf)
		recordBuf[0] = ' ' // FoxPro record delete flag: 0x20 (' ') for active, 0x2A ('*') for deleted

		offset := 1
		for i, field := range fields {
			if i >= len(record) {
				break
			}

			val := record[i]
			encodedBytes, _, _ := transform.Bytes(encoder, []byte(val))

			if len(encodedBytes) > field.Length {
				encodedBytes = encodedBytes[:field.Length]
			}
			copy(recordBuf[offset:], encodedBytes)
			offset += field.Length
		}

		if _, err := w.Write(recordBuf); err != nil {
			return err
		}

		processed++
		// [Refactor] Use flagProgress to control output
		if flagProgress > 0 && processed%uint32(flagProgress) == 0 {
			fmt.Printf("  >> Written %d / %d ...\r", processed, total)
		}
	}

	// [Refactor] Only print completion line if progress reporting was enabled
	if flagProgress > 0 {
		fmt.Printf("  >> Written %d / %d ...\n", processed, total)
	}
	return nil
}

func fillSpace(b []byte) {
	if len(b) == 0 {
		return
	}
	b[0] = ' '
	for i := 1; i < len(b); i *= 2 {
		copy(b[i:], b[:i])
	}
}
