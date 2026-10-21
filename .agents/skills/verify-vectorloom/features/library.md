# Library operators
## Sub-features
Parquet scan, filter, projection, count, grouped sums.
## How to get to it (user POV)
Import the ee2 library and construct physical operators as tests/*.rs does.
## Driving it with PowerShell
Run scripts/verify.ps1. Existing integration tests assert values over shipped data/products.parquet.
## Gotchas
The upstream interactive binary requires unavailable TPCH files. Its pipeline uses Arc<RefCell> and is serial.
