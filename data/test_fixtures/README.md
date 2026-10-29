# Negative-test data (not part of the clean starter input)

`invalid_order_items.csv` contains deliberately invalid synthetic records.
`expected_rejections.csv` is a **separate label file**, not an ETL input.

The first source file contains four invalid cases:
- line 99: quantity is zero;
- line 100: product P9999 is absent from products.csv;
- line 101: discount 101 exceeds gross 100;
- line 102: unit price is not an integer.

To manually exercise these without modifying the clean sample, copy the four clean
source CSV files into a *new* directory, then append the invalid data rows (not
its header) to the copied `order_items.csv`. Run the pipeline with `--source`
pointing at that new directory and a **separate test database path**.

The automated pytest suite already generates these and other edge-case fixtures
in temporary directories; it does not modify the starter files.
