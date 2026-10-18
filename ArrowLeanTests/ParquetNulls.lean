import ArrowLean

/-!
Regression test: nullable Parquet columns must be readable by other implementations.

`ArrowLeanTests/run-parquet-nulls-test.sh` builds this, writes a file and checks it
with pyarrow (`check_parquet_nulls.py`). Until 2026-10 the writer framed definition
levels with a 1-byte bit width instead of the 4-byte length prefix the format
requires, so every OPTIONAL column was unreadable outside arrow-lean.

The file also has one column of each 4-byte and narrow type: the writer used to store
date32 as INT64 (reading 8 bytes per 4-byte value) and had no data path for INT32 or
FLOAT columns at all.
-/

open ArrowLean

structure Row where
  id : Int64
  name : Option String
  qty : Option Int64
  px : Option Float
  tag : Option String   -- nullable but always present (the all-defined RLE run)
  day : Int32           -- date32, days since 1970-01-01
  expiry : Option Int32 -- nullable date32
  n32 : Int32
  n8 : Int8
  n16 : Int16
  f32 : Float           -- float32

instance : ToArrowBatch Row where
  columnSpecs := #[
    ColumnSpec.int64 "id" false,
    ColumnSpec.string "name" true,
    ColumnSpec.int64 "qty" true,
    ColumnSpec.float64 "px" true,
    ColumnSpec.string "tag" true,
    ColumnSpec.date32 "day" false,
    ColumnSpec.date32 "expiry" true,
    ColumnSpec.int32 "n32" false,
    ColumnSpec.int8 "n8" false,
    ColumnSpec.int16 "n16" false,
    ColumnSpec.float32 "f32" false
  ]
  buildColumns rows := do
    let id ← buildInt64Column (rows.map (·.id))
    let name ← buildOptStringColumn (rows.map (·.name))
    let qty ← buildOptInt64Column (rows.map (·.qty))
    let px ← buildOptFloat64Column (rows.map (·.px))
    let tag ← buildOptStringColumn (rows.map (·.tag))
    let day ← buildDate32Column (rows.map (·.day))
    let expiry ← do
      let some b ← Date32Builder.create rows.size.toUSize | pure none
      for r in rows do
        let _ ← b.appendOption r.expiry
      b.finish
    let n32 ← buildInt32Column (rows.map (·.n32))
    let n8 ← buildInt8Column (rows.map (·.n8))
    let n16 ← buildInt16Column (rows.map (·.n16))
    let f32 ← buildFloat32Column (rows.map (·.f32))
    match id, name, qty, px, tag, day, expiry, n32, n8, n16, f32 with
    | some a, some b, some c, some d, some e, some f, some g, some h, some i, some j, some k =>
      return some #[a, b, c, d, e, f, g, h, i, j, k]
    | _, _, _, _, _, _, _, _, _, _, _ => return none

/-- 11 rows (crosses an 8-value bit-packed group); nulls in different positions -/
def rows : Array Row := (List.range 11).toArray.map fun i =>
  { id := i.toInt64
    name := if i % 3 == 1 then none else some s!"n{i}"
    qty := if i % 4 == 2 then none else some (i.toInt64 * 10)
    px := if i == 0 || i == 10 then none else some (i.toFloat + 0.5)
    tag := some s!"t{i}"
    day := (19724 + i).toInt32                    -- 2024-01-02 + i
    expiry := if i % 5 == 3 then none else some (20000 + i).toInt32
    n32 := Int.toInt32 ((Int.ofNat i - 5) * 100000)
    n8 := Int.toInt8 (Int.ofNat i - 5)
    n16 := Int.toInt16 ((Int.ofNat i - 5) * 1000)
    f32 := i.toFloat * 0.25 }

def main (args : List String) : IO UInt32 := do
  let path := args.headD "parquet-nulls.parquet"
  if ← writeRecordsToParquet path rows then
    IO.println s!"wrote {rows.size} rows to {path}"
    return 0
  else
    IO.eprintln "writeRecordsToParquet failed"
    return 1
