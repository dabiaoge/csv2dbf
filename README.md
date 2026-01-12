# csv2dbf & dbf2csv, programs that convert between CSV and DBF formats.
- csv2dbf: support xBase III only.
- dbf2csv: support xBase III/IV/VII, xFoxPro. 
-----------------------------------------------------------------------------
# csv2dbf
```text
CSV2DBF Converter
Author : dabioage

Usage: csv2dbf [options] <csv_file1> [csv_file2] ...

Options:
  -c int
        Show progress every N rows (default 0, disable output)
  -e string
        Encoding (UTF-8, GBK, GB18030) (default "UTF-8")
  -f string
        Field delimiter (single char) (default ",")
  -l string
        Line ending (e.g. "\n", "\r\n") (default "\n")
  -q string
        Quote character (default "\"")

Examples:
  csv2dbf data.csv
  csv2dbf -e GBK -c 5000 data.csv
  csv2dbf -f '|' data.csv
```

-----------------------------------------------------------------------------

# dbf2csv
```text
DBF2CSV Converter
Author : dabiaoge

Usage: dbf2csv [options] <dbf_file1> [dbf_file2] ...

Options:
  -c int
        Show progress every N rows (default 0, disable output)
  -e string
        Source DBF Encoding (UTF-8, GBK, GB18030) (default "UTF-8")
  -f string
        Output field delimiter (single char) (default ",")
  -l string
        Output line ending (e.g. "\n", "\r\n") (default "\n")
  -q string
        Quote character (default "\"")

Examples:
  dbf2csv data.dbf
  dbf2csv -e GBK -c 5000 data.dbf
  dbf2csv -f '|' data.dbf
```
