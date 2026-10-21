$ErrorActionPreference = 'Continue'
Set-Location (Split-Path $PSScriptRoot -Parent)
New-Item -ItemType Directory -Force evidence | Out-Null
$proof = Join-Path 'evidence' ('verify-' + (Get-Date -Format 'yyyyMMdd-HHmmss') + '.log')
"Launch/Doctor: $(cargo --version)" | Tee-Object -FilePath $proof
& cargo test --locked --jobs 2 *> "$proof.tmp"
$code = $LASTEXITCODE
Get-Content "$proof.tmp" | Tee-Object -FilePath $proof -Append
Remove-Item -LiteralPath "$proof.tmp"
"Drive exit=$code; Cleanup: no persistent process or scratch data; evidence retained" | Tee-Object -FilePath $proof -Append
if ($code -ne 0) { throw "Library consumer drive failed: $code" }
if (-not (Test-Path $proof)) { throw 'Cleanup removed evidence' }

$binary = Join-Path (Get-Location) ('target/debug/ee2' + $(if ($env:OS -eq 'Windows_NT') { '.exe' } else { '' }))
if (-not (Test-Path $binary)) { throw 'Launch requires cargo build' }
$queries = @(
 'select category,count(*) as n,sum(value) as total from l group by category order by category',
 'select l.id,l.value,r.value as rv from l join r on l.id=r.id and l.category=r.category order by l.value,rv',
 'select value+1 as v from l where value>=10 order by v desc limit 3',
 'select count(*),sum(value) from l'
)
foreach ($n in @(0,35)) {
 foreach ($sql in $queries) {
  $index = [array]::IndexOf($queries,$sql)
  $out = Join-Path 'evidence' "cli-$n-$index.csv"
  $reference = Join-Path 'evidence' "reference-$n-$index.csv"
  & $binary --synthetic $n --batch-size 3 --workers 4 --sql $sql 1> $out 2> "$out.metrics"
  if ($LASTEXITCODE -ne 0) { throw "CLI failed: $sql" }
  & $binary --synthetic $n --batch-size 3 --sql $sql --reference 1> $reference 2> "$reference.metrics"
  if ($LASTEXITCODE -ne 0) { throw "Reference failed: $sql" }
  if ((Get-Content $out -Raw) -cne (Get-Content $reference -Raw)) { throw "Differential CLI mismatch: $sql" }
  "CLI passed synthetic=$n query=$index output=$out" | Tee-Object -FilePath $proof -Append
 }
}
& $binary --parquet products=data/products.parquet --sql 'select fruit,sum(price*quantity) as total from products group by fruit order by fruit' 1> evidence/parquet.csv 2> evidence/parquet.metrics
if ($LASTEXITCODE -ne 0) { throw 'Parquet CLI failed' }
$fixture = Join-Path 'evidence' 'fixture.csv'
"id,value`n1,10`n1,20`n2,30" | Set-Content $fixture
& $binary --csv "items=$fixture" --sql 'select id,sum(value) as total from items group by id order by id' 1> evidence/csv.csv 2> evidence/csv.metrics
if ($LASTEXITCODE -ne 0) { throw 'CSV CLI failed' }
$csv = Import-Csv evidence/csv.csv
if ($csv.Count -ne 2 -or $csv[0].total -ne '30' -or $csv[1].total -ne '30') { throw 'CSV aggregate values incorrect' }
Remove-Item -LiteralPath $fixture
& $binary --workers 0 --sql 'select 1' 1> evidence/invalid.csv 2> evidence/invalid.metrics
if ($LASTEXITCODE -eq 0) { throw 'Invalid workers accepted' }
& $binary --synthetic 2 --sql 'select * from l left join r on l.id=r.id' 1> evidence/unsupported.csv 2> evidence/unsupported.metrics
if ($LASTEXITCODE -eq 0) { throw 'Unsupported plan accepted' }
"Cleanup complete; scratch fixture removed; CLI evidence retained" | Tee-Object -FilePath $proof -Append
if (-not (Test-Path $proof) -or -not (Test-Path evidence/parquet.csv)) { throw 'Cleanup removed evidence' }
