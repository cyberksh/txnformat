"""
Auto formats CSV generated by Axis Bank to be GNUCash importable
"""

# TODO rewrite in Rust once you get it working in Python

import os
import re
import tempfile

import polars as pl

# TODO make an actual CLI out of it
file_path: str = os.environ["FILE_PATH"]

with open(file_path, "r") as f:
    raw_data: str = f.read()

# regex match Tran Date, CHQNO
prefix_match = re.search("Tran Date,CHQNO", raw_data)
if not prefix_match:
    raise ValueError("Something is wrong with the prefix match")
# use the first match group
prepended_cruft_deleted: str = raw_data[prefix_match.start(0) :]

# I should probably look for dates to match than this weird string but it works
suffix_match = re.search('\n"Unless the constituent notifies', prepended_cruft_deleted)
if not suffix_match:
    raise ValueError("Something is wrong with the suffix match")
suffix_cruft_deleted: str = prepended_cruft_deleted[: suffix_match.start(0) - 1]

processed_data = suffix_cruft_deleted

with tempfile.NamedTemporaryFile() as tmp:
    print(f"writing intermdiate file to: {tmp.name}")
    # Save to convert as it is text csv
    tmp.write(processed_data.encode(encoding="utf-8"))
    # NamedTemporaryFile have a `delete=True` default
    # the file is deleted as soon as it is closed.
    # we can set `delete=False` but then it relies on the cleanup that /tmp is cleared
    # after host restart or we set another delete call
    tmp.flush()
    df = pl.read_csv(source=tmp.name)

# Strip all whitespaces so that it can be converted to ints
filtered_df = df.select(
    pl.col("Tran Date"),
    pl.col("PARTICULARS"),
    pl.col("DR").str.strip().cast(pl.Float32, strict=False).fill_null(0),
    pl.col("CR").str.strip().cast(pl.Float32, strict=False).fill_null(0),
)

print(filtered_df)
filtered_df.write_csv("axis-bank-out.csv")
