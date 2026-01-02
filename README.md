# csv2dbf & dbf2csv
-----------------------------------------------------------------------------
# csv2dbf
```text
CSV2DBF Converter
Version: 1.1
Author : dabiaoge

Usage: ./csv2dbf [options] <csv_file1> [csv_file2] ...

Options:
  -c int
    	Show progress every N rows (0 = disable output)
  -e string
    	Encoding (UTF-8, GBK, GB18030) (default "UTF-8")
  -f string
    	Field delimiter (single char) (default ",")
  -l string
    	Line ending (e.g. '\n', '\r\n') (default "\\n")
  -q string
    	Quote character (ignored, standard CSV uses ") (default "\"")

Note: Standard CSV parser enforces double-quotes (") as the quote character.

Examples:
  ./csv2dbf data.csv
  ./csv2dbf -e GBK -c 5000 data.csv
  ./csv2dbf -f '|' data.csv
```

-----------------------------------------------------------------------------

# dbf2csv
```text
DBF2CSV Converter
Version: 1.1
Author : dabiaoge

Usage: ./dbf2csv [options] <dbf_file1> [dbf_file2] ...

Options:
  -c int
    	Show progress every N rows (0 = disable output)
  -e string
    	Source DBF Encoding (UTF-8, GBK, GB18030) (default "UTF-8")
  -f string
    	Output field delimiter (single char) (default ",")
  -l string
    	Output line ending (e.g. '\n', '\r\n') (default "\\n")
  -q string
    	Quote character (standard CSV uses ") (default "\"")

Note: Converts DBF (binary) to standard CSV (UTF-8).

Examples:
  ./dbf2csv data.dbf
  ./dbf2csv -e GBK -c 5000 data.dbf
  ./dbf2csv -f '|' data.dbf
```
