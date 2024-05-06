import csv


def load_us_population_data(
    filename: str = "sample-data/us_pop_500.csv",
) -> dict:
    """
    Loads a CSV file into a dictionary (manual approach).

    Args:
        filename: The path to the CSV file.

    Returns:
        A dictionary where keys are column names (from first row) and values are lists of corresponding values.
    """
    with open(filename, "r") as csv_file:
        reader = csv.reader(csv_file)
        headers = next(reader)  # Read the first row as headers
        data = list(reader)
        return {h: [row[i] for row in data] for i, h in enumerate(headers)}
