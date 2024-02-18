import pathlib
import random
import re
import string
import tempfile
import typing

import polars as pl
import typer


def read_cc_data(file_path: pathlib.Path) -> str:
    with open(file_path, "r") as f:
        return f.read()


def cleanup_cc_data(data: str) -> str:
    prefix_match = re.search('"4315XXXXXXXX7008"', data)
    if not prefix_match:
        raise ValueError("Something is wrong with prefix match")
    prepended_cruft_deleted: str = data[prefix_match.end(0) :]
    return prepended_cruft_deleted


def process_icici_cc(file_path: pathlib.Path) -> None:
    raw_data = read_cc_data(file_path=file_path)
    formatted_data = cleanup_cc_data(raw_data)
    with tempfile.NamedTemporaryFile() as tmp:
        tmp.write(formatted_data.encode())
        tmp.flush()
        df = pl.read_csv(
            tmp.name,
            has_header=False,
            new_columns=[
                "date",
                "id",
                "description",
                "x",
                "y",
                "value",
                "type",
            ],
        )

    filtered_df = df.select(
        pl.col("date").str.to_date("%d/%m/%Y"),
        pl.col("description"),
        pl.when(pl.col("type") == "")
        .then(pl.col("value"))
        .otherwise(pl.lit(0))
        .alias("debit"),
        pl.when(pl.col("type") == "CR")
        .then(pl.col("value"))
        .otherwise(pl.lit(0))
        .alias("credit"),
    )

    typer.echo(filtered_df)
    random_file_suffix = "".join(random.choices(string.ascii_lowercase, k=5))
    output_path = f"{file_path.parent}/icici-cc-out-{random_file_suffix}.csv"
    typer.echo(f"Writing results to: {output_path}")
    filtered_df.write_csv(file=output_path, date_format="%Y-%m-%d")


def main(
    file_path: typing.Annotated[
        pathlib.Path, typer.Argument(help="Path to transactions file")
    ],
):
    process_icici_cc(file_path=file_path)


if __name__ == "__main__":
    typer.run(main)
