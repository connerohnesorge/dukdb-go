## 1. Parser Lexer/Keyword Updates
- [ ] Add lexer recognition for EXCEL-specific keywords if needed (sheet, range etc. as idents)
- [ ] Extend parseTableFunction to validate/normalize read_excel* named args (sheet=str, range=str, header=bool etc.)

## 2. AST Enhancements
- [ ] Ensure TableFunctionRef.NamedArgs handles Excel options (sheet, range, usecols, names, header_row, header, skip_rows, skipfooter, nrows, dtype, na_values, keep_default_na, na_filter, verbose, parse_dates, date_parser, dayfirst, cache_parser, thousands, decimal, skip_blank_lines, chunksize, on_bad_lines, nrows_dtype, low_memory)
- [ ] Add validation for Excel-specific args in binder (later)

## 3. Tests
- [ ] Add parser_test.go cases for read_excel('file.xlsx'), read_excel_auto(), all options
- [ ] Test param counting in Excel table funcs
- [ ] Test TableExtractor extracts Excel as table ref

## 4. Validation & Docs
- [ ] Run `spectr validate add-excel-table-function-parser-support`
- [ ] Update README parser section w/ Excel example
- [ ] Archive change