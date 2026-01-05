package main

import (
	"bufio"
	"bytes"
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
	flagProgress  int // Control progress reporting interval
)

// Constants for program info
const (
	AppVersion = "1.2.0" // Matched version
	AppAuthor  = "dabiaoge"
)

// DBFHeader represents the file header structure (32 bytes)
type DBFHeader struct {
	Version   byte     // 0-0
	Year      byte     // 1-1 (Year - 1900)
	Month     byte     // 2-2
	Day       byte     // 3-3
	NumRecs   uint32   // 4-7
	HeaderLen uint16   // 8-9 (32 + 32*n + 1)
	RecLen    uint16   // 10-11
	Reserved  [20]byte // 12-31
}

// DBFField represents the field descriptor structure (32 bytes)
type DBFField struct {
	Name      [11]byte // 0-10
	Type      byte     // 11-11
	Reserved  [4]byte  // 12-15
	Len       byte     // 16-16
	Dec       byte     // 17-17
	Reserved2 [14]byte // 18-31
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
	flag.StringVar(&flagDelimiter, "f", ",", "Output field delimiter (single char)")
	flag.StringVar(&flagQuote, "q", "\"", "Quote character")
	flag.StringVar(&flagNewline, "l", "\n", "Output line ending (e.g. \"\\n\", \"\\r\\n\")")
	flag.StringVar(&flagEncoding, "e", "UTF-8", "Source DBF Encoding (UTF-8, GBK, GB18030)")
	flag.IntVar(&flagProgress, "c", 0, "Show progress every N rows (default 0, disable output)")

	// Custom usage message
	flag.Usage = func() {
		fmt.Printf("DBF2CSV Converter\n")
		fmt.Printf("Version: %s\n", AppVersion)
		fmt.Printf("Author : %s\n\n", AppAuthor)
		fmt.Printf("Usage: %s [options] <dbf_file1> [dbf_file2] ...\n\n", os.Args[0])
		fmt.Println("Options:")
		flag.PrintDefaults()
		fmt.Println("\nExamples:")
		fmt.Printf("  %s data.dbf\n", os.Args[0])
		fmt.Printf("  %s -e GBK -c 5000 data.dbf\n", os.Args[0])
		fmt.Printf("  %s -f '|' data.dbf\n", os.Args[0])
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
	// newline handling for CSV writer is limited in stdlib, but we handle basic CRLF check later

	// Determine encoding
	enc := getEncoding(flagEncoding)
	if enc == nil {
		fmt.Fprintf(os.Stderr, "Error: Unsupported encoding '%s'\n", flagEncoding)
		os.Exit(1)
	}

	for _, dbfFile := range args {
		if _, err := os.Stat(dbfFile); os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "Error: File not found [%s]\n", dbfFile)
			continue
		}

		fmt.Printf("Processing: %s\n", dbfFile)
		startTime := time.Now()

		err := convertDBFtoCSV(dbfFile, delimiter, enc)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed [%s]: %v\n", dbfFile, err)
			continue
		}

		elapsed := time.Since(startTime)
		fmt.Printf("Done: %s (Time: %.3fs)\n", dbfFile, elapsed.Seconds())
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
		}
	}
	r, _ := utf8.DecodeRuneInString(s)
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

func convertDBFtoCSV(dbfPath string, comma rune, enc encoding.Encoding) error {
	// --- Pass 1: Read Structure ---
	fmt.Println("  [1/2] Reading DBF structure...")

	f, err := os.Open(dbfPath)
	if err != nil {
		return err
	}
	defer f.Close()

	header, fields, err := readDBFHeaderAndFields(f, enc)
	if err != nil {
		return err
	}
	fmt.Printf("  >> Fields: %d, Records: %d\n", len(fields), header.NumRecs)

	// --- Prepare CSV File ---
	csvPath := strings.TrimSuffix(dbfPath, filepath.Ext(dbfPath)) + ".csv"
	csvFile, err := os.Create(csvPath)
	if err != nil {
		return fmt.Errorf("failed to create CSV: %w", err)
	}
	defer csvFile.Close()

	// Setup CSV Writer
	// Note: We use a buffer to improve write performance
	bufWriter := bufio.NewWriterSize(csvFile, 4*1024*1024)
	w := csv.NewWriter(bufWriter)
	w.Comma = comma

	// Handle newline flag roughly (encoding/csv mostly supports \n or \r\n via UseCRLF)
	if strings.Contains(flagNewline, "\\r\\n") {
		w.UseCRLF = true
	}

	// --- Write CSV Header ---
	var headerRow []string
	for _, field := range fields {
		headerRow = append(headerRow, field.Name)
	}
	if err := w.Write(headerRow); err != nil {
		return err
	}

	// --- Pass 2: Read Data & Write ---
	fmt.Println("  [2/2] Exporting records...")

	// Move file pointer to start of data
	if _, err := f.Seek(int64(header.HeaderLen), 0); err != nil {
		return fmt.Errorf("failed to seek to data: %w", err)
	}

	if err := writeCSVRecords(f, w, header, fields, enc); err != nil {
		return err
	}

	w.Flush()
	return bufWriter.Flush()
}

func readDBFHeaderAndFields(r io.Reader, enc encoding.Encoding) (DBFHeader, []FieldInfo, error) {
	var h DBFHeader
	if err := binary.Read(r, binary.LittleEndian, &h); err != nil {
		return h, nil, fmt.Errorf("failed to read header: %w", err)
	}

	// Sanity check
	if h.HeaderLen < 32 {
		return h, nil, fmt.Errorf("invalid header length")
	}

	// Number of fields = (HeaderLen - 32 (Header) - 1 (Terminator)) / 32 (FieldDesc)
	numFields := int(h.HeaderLen-32-1) / 32
	fields := make([]FieldInfo, numFields)

	decoder := enc.NewDecoder()

	for i := 0; i < numFields; i++ {
		var df DBFField
		if err := binary.Read(r, binary.LittleEndian, &df); err != nil {
			return h, nil, fmt.Errorf("failed to read field %d: %w", i, err)
		}

		// Clean field name (remove nulls)
		rawName := bytes.TrimRight(df.Name[:], "\x00")

		// Decode field name (in case DBF field names use specific encoding, though usually ASCII)
		// Usually DBF field names are ASCII, but we play safe if users use -e
		nameStr, _, _ := transform.Bytes(decoder, rawName)

		fields[i] = FieldInfo{
			Name:   string(nameStr),
			Type:   df.Type,
			Length: int(df.Len),
			Dec:    int(df.Dec),
		}
	}

	return h, fields, nil
}

func writeCSVRecords(r io.Reader, w *csv.Writer, h DBFHeader, fields []FieldInfo, enc encoding.Encoding) error {
	recordBuf := make([]byte, h.RecLen)
	row := make([]string, len(fields))
	decoder := enc.NewDecoder()

	var processed uint32

	for i := uint32(0); i < h.NumRecs; i++ {
		// Read exact record length
		_, err := io.ReadFull(r, recordBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading record %d: %w", i, err)
		}

		// Check deletion flag (Byte 0)
		// 0x2A ('*') means deleted, 0x20 (' ') means active
		if recordBuf[0] == 0x2A {
			// Skip deleted records
			processed++ // Still counts towards file progress
			continue
		}

		offset := 1 // Start after deletion flag
		for j, field := range fields {
			if offset+field.Length > len(recordBuf) {
				break
			}

			// Extract raw bytes for field
			rawField := recordBuf[offset : offset+field.Length]

			// Trim spaces (DBF pads with spaces)
			// Trimming must happen *after* decoding ideally, but for GBK/UTF8
			// spaces are usually safe to trim before if standard ASCII space.
			// However, correct flow is Decode -> Trim to handle multibyte spaces if any.

			decodedBytes, _, err := transform.Bytes(decoder, rawField)
			if err != nil {
				// Fallback to raw bytes if decode fails
				row[j] = strings.TrimSpace(string(rawField))
			} else {
				row[j] = strings.TrimSpace(string(decodedBytes))
			}

			offset += field.Length
		}

		if err := w.Write(row); err != nil {
			return err
		}

		processed++
		if flagProgress > 0 && processed%uint32(flagProgress) == 0 {
			fmt.Printf("  >> Exported %d / %d ...\r", processed, h.NumRecs)
		}
	}

	if flagProgress > 0 {
		fmt.Printf("  >> Exported %d / %d ...\n", processed, h.NumRecs)
	}
	return nil
}
