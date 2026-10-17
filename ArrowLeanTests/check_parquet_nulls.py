# Check the file written by ArrowLeanTests/ParquetNulls.lean with pyarrow.
import sys
import pyarrow.parquet as pq

expected = []
for i in range(11):
    expected.append({
        "id": i,
        "name": None if i % 3 == 1 else f"n{i}",
        "qty": None if i % 4 == 2 else i * 10,
        "px": None if i in (0, 10) else i + 0.5,
        "tag": f"t{i}",
    })

got = pq.read_table(sys.argv[1]).to_pylist()
if got == expected:
    print(f"pyarrow reads all {len(got)} rows correctly, nulls included")
    sys.exit(0)
print("MISMATCH")
for e, g in zip(expected, got):
    if e != g:
        print("  expected", e, "\n  got     ", g)
sys.exit(1)
