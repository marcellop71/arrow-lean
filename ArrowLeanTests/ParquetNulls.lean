import ArrowLean

/-!
Regression test: nullable Parquet columns must be readable by other implementations.

`ArrowLeanTests/run-parquet-nulls-test.sh` builds this, writes a file and checks it
with pyarrow (`check_parquet_nulls.py`). Until 2026-10 the writer framed definition
levels with a 1-byte bit width instead of the 4-byte length prefix the format
requires, so every OPTIONAL column was unreadable outside arrow-lean.
-/

open ArrowLean

structure Row where
  id : Int64
  name : Option String
  qty : Option Int64
  px : Option Float
  tag : Option String   -- nullable but always present (the all-defined RLE run)

instance : ToArrowBatch Row where
  columnSpecs := #[
    ColumnSpec.int64 "id" false,
    ColumnSpec.string "name" true,
    ColumnSpec.int64 "qty" true,
    ColumnSpec.float64 "px" true,
    ColumnSpec.string "tag" true
  ]
  buildColumns rows := do
    let id ← buildInt64Column (rows.map (·.id))
    let name ← buildOptStringColumn (rows.map (·.name))
    let qty ← buildOptInt64Column (rows.map (·.qty))
    let px ← buildOptFloat64Column (rows.map (·.px))
    let tag ← buildOptStringColumn (rows.map (·.tag))
    match id, name, qty, px, tag with
    | some a, some b, some c, some d, some e => return some #[a, b, c, d, e]
    | _, _, _, _, _ => return none

/-- 11 rows (crosses an 8-value bit-packed group); nulls in different positions -/
def rows : Array Row := (List.range 11).toArray.map fun i =>
  { id := i.toInt64
    name := if i % 3 == 1 then none else some s!"n{i}"
    qty := if i % 4 == 2 then none else some (i.toInt64 * 10)
    px := if i == 0 || i == 10 then none else some (i.toFloat + 0.5)
    tag := some s!"t{i}" }

def main (args : List String) : IO UInt32 := do
  let path := args.headD "parquet-nulls.parquet"
  if ← writeRecordsToParquet path rows then
    IO.println s!"wrote {rows.size} rows to {path}"
    return 0
  else
    IO.eprintln "writeRecordsToParquet failed"
    return 1
