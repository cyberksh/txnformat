import pathlib
import re
import tempfile
import typing

import polars as pl
import typer


def cleanup_data(data: str) -> str:
    prefix_match = re.search("Txn Date", data)
    if not prefix_match:
        raise ValueError("Prefix match failed with data")
    prepended_cruft_deleted: str = data[prefix_match.start(0) :]

    suffix_match = re.search(r"\*\*This is a", prepended_cruft_deleted)
    if not suffix_match:
        raise ValueError("Suffix match failed with data")
    suffix_cruft_deleted: str = prepended_cruft_deleted[: suffix_match.start(0) - 1]

    commas_removed_data = re.sub(",", "", suffix_cruft_deleted)
    tabs_replaced_data = re.sub("\t", ",", commas_removed_data)

    # Handle special case
    remove_spaces_debit = re.sub("No.,( )+Debit", "No.,Debit", tabs_replaced_data)
    return remove_spaces_debit


def convert_txn_data_to_float(col_name: str) -> pl.Expr:
    return (
        pl.col(col_name).str.strip_chars().cast(pl.Float32, strict=False).fill_null(0)
    )


def convert_txn_data_to_date(col_name: str) -> pl.Expr:
    return pl.col(col_name).str.to_date("%d %b %Y")


def main(
    file_path: typing.Annotated[
        pathlib.Path, typer.Argument(help="Path to transactions file")
    ],
):
    with open(file_path, "r") as f:
        raw_data: str = f.read()

    formatted_data = cleanup_data(data=raw_data)

    with tempfile.NamedTemporaryFile() as tmp:
        tmp.write(formatted_data.encode(encoding="utf-8"))
        tmp.flush()
        df = pl.read_csv(tmp.name)

    filtered_df = df.select(
        convert_txn_data_to_date(col_name="Txn Date"),
        pl.col("Description"),
        convert_txn_data_to_float(col_name="Debit"),
        convert_txn_data_to_float(col_name="Credit"),
    )

    typer.echo(filtered_df)
    filtered_df.write_csv(
        file=f"{file_path.parent}/sbi-out.csv", date_format="%Y-%m-%d"
    )
    typer.echo("File written to sbi-out.csv")


if __name__ == "__main__":
    typer.run(main)
