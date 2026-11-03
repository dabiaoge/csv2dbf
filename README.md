# csv2dbf & dbf2csv, command-line tools that convert between CSV and DBF formats.
- csv2dbf: dbf file format fixed to dBase IV(FoxPro 2.x)
- dbf2csv: support dBase III/IV/VII, FoxPro 2.0-2.6, Visual FoxPro 3.0-9.0
-----------------------------------------------------------------------------
# csv2dbf
```text
CSV2DBF Converter
Author : dabiaoge

Usage: csv2dbf [options] <csv_file1> [csv_file2] ...

Options:
  -n int
        Show progress every N rows (default 0, disable output)
  -e string
        Encoding (GBK, GB18030, UTF-8) (default "GBK")
  -f string
        Field delimiter (single char) (default ",")
  -l string
        Line ending (e.g. "\n", "\r\n") (default "\n")
  -q string
        Quote character (default "\"")

Examples:
  csv2dbf data.csv
  csv2dbf -e GBK -n 10000 data.csv
  csv2dbf -f '|' data.csv
```

-----------------------------------------------------------------------------

# dbf2csv
```text
DBF2CSV Converter
Author : dabiaoge

Usage: dbf2csv [options] <dbf_file1> [dbf_file2] ...

Options:
  -n int
        Show progress every N rows (default 0, disable output)
  -e string
        Source DBF Encoding (GBK, GB18030, UTF-8) (default "GBK")
  -f string
        Output field delimiter (single char) (default ",")
  -l string
        Output line ending (e.g. "\n", "\r\n") (default "\n")
  -q string
        Quote character (default "\"")

Examples:
  dbf2csv data.dbf
  dbf2csv -e GBK -n 10000 data.dbf
  dbf2csv -f '|' data.dbf
```
