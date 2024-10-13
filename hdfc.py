import pathlib
import random
import re
import string
import tempfile
import typing

import polars as pl
import typer


def read_csv_data(file_path: pathlib.Path) -> str:
    """Reads raw CSV data from a file."""
    with open(file_path, "r") as f:
        return f.read()


def cleanup_data(data: str) -> str:
    """Clean up the input CSV data to focus on the transaction section."""
    # Look for the 'Transaction type' header and remove everything before it
    transaction_start_match = re.search("Transaction type~Primary", data)
    if not transaction_start_match:
        raise ValueError("Could not find the start of the transaction details section.")
    cleaned_data = data[transaction_start_match.start(0) :]

    transaction_end_match = re.search("Opening Bal~Base", cleaned_data)
    if not transaction_end_match:
        raise ValueError("Could not find the end of the transaction details")
    cleaned_data = cleaned_data[: transaction_end_match.start()]

    cleaned_data = cleaned_data.strip()
    return cleaned_data


def process_bank_statement(file_path: pathlib.Path) -> None:
    """Processes the CSV file and converts it to a structured format."""
    raw_data = read_csv_data(file_path=file_path)
    cleaned_data = cleanup_data(raw_data)

    # Write cleaned data to a temporary file to load it with polars
    with tempfile.NamedTemporaryFile() as tmp:
        tmp.write(cleaned_data.encode())
        tmp.flush()
        df = pl.read_csv(
            tmp.name,
            separator="~",  # using '~' as the separator based on the input format
            has_header=True,
            new_columns=[
                "transaction_type",
                "customer_name",
                "date",
                "description",
                "reward_points",
                "amount",
                "credit_or_debit",
            ],
        )

    # Filter the dataframe and clean up the transaction records
    filtered_df = df.select(
        pl.col("date").str.strptime(
            pl.Date, format="%d/%m/%Y", strict=False, exact=False
        ),
        pl.col("description"),
        pl.when(pl.col("credit_or_debit").str.contains("Cr"))
        .then(pl.col("amount"))
        .otherwise(pl.lit(0))
        .alias("credit"),
        pl.when(~pl.col("credit_or_debit").str.contains("Cr"))
        .then(pl.col("amount"))
        .otherwise(pl.lit(0))
        .alias("debit"),
    )

    typer.echo(filtered_df)

    # Generate a random file name suffix
    random_file_suffix = "".join(random.choices(string.ascii_lowercase, k=5))
    output_path = f"{file_path.parent}/hdfc-cc-out-{random_file_suffix}.csv"

    # Write the processed data to a new CSV file
    typer.echo(f"Writing results to: {output_path}")
    filtered_df.write_csv(file=output_path, date_format="%Y-%m-%d")


def main(
    file_path: typing.Annotated[
        pathlib.Path, typer.Argument(help="Path to the CSV file")
    ],
):
    process_bank_statement(file_path=file_path)


if __name__ == "__main__":
    typer.run(main)
