import pathlib
import random
import re
import string
import tempfile
import typing

import polars as pl
import typer
import xlsx2csv as x2csv


def cleanup_cc_data(data: str):
    prefix_match = re.search("Date,Transaction", data)
    if not prefix_match:
        raise ValueError("Prefix match failed for credit card")
    prefix_deleted = data[prefix_match.start(0) :]

    suffix_match = re.search("\*\* End of", prefix_deleted)
    if not suffix_match:
        raise ValueError("Suffix match failed for credit card")
    suffix_deleted = prefix_deleted[: suffix_match.start(0) - 1]
    return suffix_deleted


def cleanup_bank_data(data: str) -> str:
    prefix_match = re.search("Tran Date,CHQNO", data)
    if not prefix_match:
        raise ValueError("Something is wrong with the prefix match")
    prepended_cruft_deleted: str = data[prefix_match.start(0) :]

    suffix_match = re.search(
        '\n"Unless the constituent notifies', prepended_cruft_deleted
    )
    if not suffix_match:
        raise ValueError("Something is wrong with the suffix match")
    suffix_cruft_deleted: str = prepended_cruft_deleted[
        : suffix_match.start(0) - 1
    ]
    return suffix_cruft_deleted


def convert_txn_data_to_float(col_name: str) -> pl.Expr:
    return (
        pl.col(col_name).str.strip().cast(pl.Float32, strict=False).fill_null(0)
    )


def convert_txn_data_to_date(col_name: str) -> pl.Expr:
    return pl.col(col_name).str.to_date("%d-%m-%Y")


def read_cc_data(file_path: pathlib.Path) -> str:
    with tempfile.NamedTemporaryFile() as tmp:
        x2csv.Xlsx2csv(file_path, outputencoding="utf-8").convert(tmp.name)
        tmp.flush()
        with open(tmp.name, "r") as f:
            return f.read()


def read_bank_data(file_path: pathlib.Path) -> str:
    with open(file_path, "r") as f:
        return f.read()


def process_axis_cc(file_path: pathlib.Path) -> None:
    raw_data = read_cc_data(file_path=file_path)
    formatted_data = cleanup_cc_data(raw_data)
    with tempfile.NamedTemporaryFile() as tmp:
        tmp.write(formatted_data.encode(encoding="utf-8"))
        tmp.flush()
        df = pl.read_csv(tmp.name)

    filtered_df = df.select(
        pl.col("Date").str.to_date("%d %b '%y"),
        pl.col("Transaction Details"),
        pl.when(pl.col("Debit/Credit") == "Debit")
        .then(
            pl.col("Amount (INR)")
            .str.replace_all(",", "")
            .str.extract(r"(\d+)")
        )
        .otherwise(pl.lit(0))
        .alias("Debit"),
        pl.when(pl.col("Debit/Credit") == "Credit")
        .then(
            pl.col("Amount (INR)")
            .str.replace_all(",", "")
            .str.extract(r"(\d+)")
        )
        .otherwise(pl.lit(0))
        .alias("Credit"),
        pl.col("Debit/Credit"),
    )
    typer.echo(filtered_df)

    random_file_suffix = "".join(random.choices(string.ascii_lowercase, k=5))
    output_path = f"{file_path.parent}/axis-cc-out-{random_file_suffix}.csv"
    typer.echo(f"Writing results to: {output_path}")
    filtered_df.write_csv(file=output_path, date_format="%Y-%m-%d")


def process_axis_bank(file_path: pathlib.Path) -> None:
    raw_data = read_bank_data(file_path=file_path)
    formatted_data = cleanup_bank_data(raw_data)
    with tempfile.NamedTemporaryFile() as tmp:
        tmp.write(formatted_data.encode(encoding="utf-8"))
        tmp.flush()
        df = pl.read_csv(tmp.name)

    filtered_df = df.select(
        convert_txn_data_to_date("Tran Date"),
        pl.col("PARTICULARS"),
        convert_txn_data_to_float("DR"),
        convert_txn_data_to_float("CR"),
    )

    typer.echo(filtered_df)

    random_file_suffix = "".join(random.choices(string.ascii_lowercase, k=5))
    output_path = f"{file_path.parent}/axis-out-{random_file_suffix}.csv"
    typer.echo(f"Writing results to: {output_path}")
    filtered_df.write_csv(file=output_path, date_format="%Y-%m-%d")


def main(
    file_path: typing.Annotated[
        pathlib.Path, typer.Argument(help="Path to transactions file")
    ],
    is_credit_card: typing.Annotated[
        bool, typer.Option("-c", help="Process credit card transactions")
    ] = False,
):
    if is_credit_card:
        process_axis_cc(file_path=file_path)
    else:
        process_axis_bank(file_path=file_path)


if __name__ == "__main__":
    typer.run(main)
