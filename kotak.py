import pathlib
import random
import re
import string
import tempfile
import typing

import polars as pl
import typer


def cleanup_bank_data(data: str) -> str:
    prefix_match = re.search("Sl. No.,Transaction Date,", data)
    if not prefix_match:
        raise ValueError("Prefix match failed for bank statement")
    prefix_deleted = data[prefix_match.start(0) :]

    suffix_match = re.search("Opening balance,", prefix_deleted)
    if not suffix_match:
        raise ValueError("Suffix match failed for bank statement")
    suffix_deleted = prefix_deleted[: suffix_match.start(0) - 1]
    return suffix_deleted


def read_bank_data(file_path: pathlib.Path) -> str:
    with open(file_path, "r") as f:
        return f.read()


def convert_txn_data_to_float(col_name: str) -> pl.Expr:
    return (
        pl.col(col_name)
        .str.replace_all(",", "")
        .str.strip()
        .cast(pl.Float32, strict=False)
        .fill_null(0)
    )


def convert_txn_data_to_date(col_name: str) -> pl.Expr:
    return pl.col(col_name).str.to_date("%d-%m-%Y")


def process_kotak_bank(file_path: pathlib.Path) -> None:
    raw_data = read_bank_data(file_path=file_path)
    formatted_data = cleanup_bank_data(raw_data)
    with tempfile.NamedTemporaryFile() as tmp:
        tmp.write(formatted_data.encode(encoding="utf-8"))
        tmp.flush()
        df = pl.read_csv(tmp.name)

    filtered_df = df.select(
        convert_txn_data_to_date("Transaction Date"),
        pl.col("Description"),
        convert_txn_data_to_float("Debit"),
        convert_txn_data_to_float("Credit"),
    )
    typer.echo(filtered_df)

    random_file_suffix = "".join(random.choices(string.ascii_lowercase, k=5))
    output_path = f"{file_path.parent}/kotak-out-{random_file_suffix}.csv"
    typer.echo(f"Writing results to: {output_path}")
    filtered_df.write_csv(file=output_path, date_format="%Y-%m-%d")


def main(
    file_path: typing.Annotated[
        pathlib.Path, typer.Argument(help="Path to transactions file")
    ],
):
    process_kotak_bank(file_path=file_path)


if __name__ == "__main__":
    typer.run(main)
