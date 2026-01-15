package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"math"
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
	AppVersion = "1.6.0"
	AppAuthor  = "dabiaoge"
)

// DBFHeader represents the file header structure (32 bytes)
type DBFHeader struct {
	Version   byte     // 0-0
	Year      byte     // 1-1 (Year - 1900)
	Month     byte     // 2-2
	Day       byte     // 3-3
	NumRecs   uint32   // 4-7
	HeaderLen uint16   // 8-9 (Position of first record)
	RecLen    uint16   // 10-11
	Reserved  [20]byte // 12-31
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
	f, err := os.Open(dbfPath)
	if err != nil {
		return err
	}
	defer f.Close()

	header, fields, err := readStructure(f, enc)
	if err != nil {
		return err
	}
	fmt.Printf("  >> Version: 0x%02X, Records: %d, Fields: %d\n", header.Version, header.NumRecs, len(fields))

	// --- Prepare CSV File ---
	csvPath := strings.TrimSuffix(dbfPath, filepath.Ext(dbfPath)) + ".csv"
	csvFile, err := os.Create(csvPath)
	if err != nil {
		return fmt.Errorf("failed to create CSV: %w", err)
	}
	defer csvFile.Close()

	encodedWriter := transform.NewWriter(csvFile, enc.NewEncoder())

	// Setup CSV Writer with buffer

	bufWriter := bufio.NewWriterSize(encodedWriter, 4*1024*1024)
	w := csv.NewWriter(bufWriter)
	w.Comma = comma

	if strings.Contains(flagNewline, "\r\n") {
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
	// Important: Seek exactly to HeaderLen.
	// VFP files have a 263+ bytes backlink area between the field terminator (0x0D)
	// and the actual data start. We must skip this area.
	if _, err := f.Seek(int64(header.HeaderLen), 0); err != nil {
		return fmt.Errorf("failed to seek to data: %w", err)
	}

	if err := writeRecords(f, w, header, fields, enc); err != nil {
		return err
	}

	w.Flush()
	return bufWriter.Flush()
}

// readStructure reads the DBF header and field definitions.
// OPTIMIZATION: Instead of calculating field count from HeaderLen (which causes ghost columns in VFP),
// we loop reading fields until the 0x0D terminator is found.
func readStructure(r io.Reader, enc encoding.Encoding) (DBFHeader, []FieldInfo, error) {
	var h DBFHeader
	if err := binary.Read(r, binary.LittleEndian, &h); err != nil {
		return h, nil, fmt.Errorf("failed to read header: %w", err)
	}

	// Sanity check
	if h.HeaderLen < 32 {
		return h, nil, fmt.Errorf("invalid header length")
	}

	var fields []FieldInfo
	decoder := enc.NewDecoder()
	maxFields := 4096 // Safety limit to prevent infinite loops on corrupted files

	for i := 0; i < maxFields; i++ {
		// Read first byte to check for terminator (0x0D)
		var marker [1]byte
		if _, err := r.Read(marker[:]); err != nil {
			return h, nil, fmt.Errorf("error reading field marker: %w", err)
		}

		if marker[0] == 0x0D {
			// End of field definitions
			break
		}

		// Read remaining 31 bytes of the 32-byte field structure
		var remaining [31]byte
		if _, err := io.ReadFull(r, remaining[:]); err != nil {
			return h, nil, fmt.Errorf("error reading field definition: %w", err)
		}

		// Reconstruct buffer
		fieldBuf := append(marker[:], remaining[:]...)

		// Field Name (bytes 0-10)
		rawName := bytes.TrimRight(fieldBuf[0:11], "\x00")
		// Use decoder for field names (usually ASCII, but helps with specific encodings)
		nameStr, _, _ := transform.Bytes(decoder, rawName)

		// Create field info
		// Byte 11: Type, Byte 16: Length, Byte 17: Decimal count
		info := FieldInfo{
			Name:   string(nameStr),
			Type:   fieldBuf[11],
			Length: int(fieldBuf[16]),
			Dec:    int(fieldBuf[17]),
		}
		fields = append(fields, info)
	}

	return h, fields, nil
}

func writeRecords(r io.Reader, w *csv.Writer, h DBFHeader, fields []FieldInfo, enc encoding.Encoding) error {
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

		// Check deletion flag (Byte 0): 0x2A ('*') means deleted.
		// We export deleted records as well, but this logic can be modified to skip them.

		offset := 1 // Start after deletion flag
		for j, field := range fields {
			if offset+field.Length > len(recordBuf) {
				break
			}

			// Extract raw bytes for field
			rawField := recordBuf[offset : offset+field.Length]

			// Parse data based on VFP/DBF field types
			row[j] = parseFieldData(rawField, field, decoder)

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

// parseFieldData converts raw bytes to string based on DBF field type.
// Supports VFP specific types (Integer, Currency, Double, DateTime).
func parseFieldData(raw []byte, f FieldInfo, decoder *encoding.Decoder) string {
	switch f.Type {
	case 'I': // Integer (4 bytes, Little Endian) - VFP
		if len(raw) == 4 {
			val := int32(binary.LittleEndian.Uint32(raw))
			return fmt.Sprintf("%d", val)
		}
		return ""

	case 'Y': // Currency (8 bytes, int64 scaled by 10000) - VFP
		if len(raw) == 8 {
			val := int64(binary.LittleEndian.Uint64(raw))
			return fmt.Sprintf("%.4f", float64(val)/10000.0)
		}
		return ""

	case 'B': // Double (8 bytes IEEE 754) - VFP
		if len(raw) == 8 {
			bits := binary.LittleEndian.Uint64(raw)
			val := math.Float64frombits(bits)
			return fmt.Sprintf("%v", val)
		}
		return ""

	case 'T': // DateTime (8 bytes) - VFP
		if len(raw) == 8 {
			julianDay := binary.LittleEndian.Uint32(raw[:4])
			millis := binary.LittleEndian.Uint32(raw[4:])

			if julianDay == 0 && millis == 0 {
				return ""
			}
			t := julianDayToTime(int(julianDay), int(millis))
			return t.Format("2006-01-02 15:04:05")
		}
		return ""

	case 'D': // Date (ASCII YYYYMMDD)
		s := string(raw)
		if len(s) == 8 && strings.TrimSpace(s) != "" {
			return fmt.Sprintf("%s-%s-%s", s[0:4], s[4:6], s[6:8])
		}
		return strings.TrimSpace(s)

	case 'L': // Logical
		s := strings.ToUpper(string(raw))
		if s == "Y" || s == "T" {
			return "TRUE"
		} else if s == "N" || s == "F" {
			return "FALSE"
		}
		return ""

	case 'M', 'G': // Memo / General (OLE)
		// Data stored in external .fpt/.dbt file.
		// This converter only handles the main .dbf file.
		return "[MEMO/OLE]"

	case 'F', 'N': // Numeric / Float (ASCII)
		return strings.TrimSpace(string(raw))

	default: // Character (C) and others
		// Optimization: Decode first, THEN trim.
		// Trimming raw bytes before decoding corrupts multi-byte encodings (like GBK)
		// where a trailing byte might legally be 0x20.

		// 1. Decode bytes using specified encoding
		decodedBytes, _, err := transform.Bytes(decoder, raw)
		strVal := ""
		if err != nil {
			// Fallback to raw string if decoding fails
			strVal = string(raw)
		} else {
			strVal = string(decodedBytes)
		}

		// 2. Remove VFP null terminators and surrounding spaces
		return strings.TrimSpace(strings.TrimRight(strVal, "\x00"))
	}
}

// julianDayToTime converts VFP Julian Day + Milliseconds to Go Time.
// Algorithm based on Fliegel and Van Flandern (1968).
func julianDayToTime(jd int, millis int) time.Time {
	l := jd + 68569
	n := (4 * l) / 146097
	l = l - (146097*n+3)/4
	i := (4000 * (l + 1)) / 1461001
	l = l - (1461*i)/4 + 31
	j := (80 * l) / 2447
	d := l - (2447*j)/80
	l = j / 11
	m := j + 2 - 12*l
	y := 100*(n-49) + i + l

	seconds := millis / 1000
	return time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC).Add(time.Duration(seconds) * time.Second)
}
