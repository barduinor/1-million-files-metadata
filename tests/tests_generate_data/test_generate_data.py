import os
from src.gen_sample_data.generate_data import (
    load_us_population_data,
    generate_customer_data,
    generate_pdf,
)
from datetime import date


def test_load_us_population_data():
    # Arrange
    us_population_data = load_us_population_data()
    # got 7 columns
    assert len(us_population_data) == 7
    # got 51 states
    assert len(us_population_data["State"]) == 51
    # first state is alabama
    assert us_population_data["State"][0] == "Alabama"


def test_generate_customer_data():
    # Arrange
    customer_id = "AA-001"
    state = "Alabama"
    postal = "AL"
    date = "2021-01-01"

    customer_data = generate_customer_data(customer_id=customer_id, state=state, postal=postal, date=date)

    # assertions
    assert customer_data["Customer ID"] == customer_id
    assert customer_data["State"] == state
    assert customer_data["Postal"] == postal
    assert customer_data["Date"] == date

    # assert it has more than 1 transaction
    assert len(customer_data["Transactions"]) > 1

    # assert the sum of each transaction matches the total on the document
    total = sum([transaction["Amount"] for transaction in customer_data["Transactions"]])
    assert abs(round(total, 2) - customer_data["Total"]) < 0.0001


def test_generate_pdf():
    # Arrange
    customer_id = "AA-001"
    state = "Alabama"
    postal = "AL"
    today = date.today()  # Get today's date
    statement_date = date(today.year, today.month, 1)
    PDF_PATH = "sample-data/files"

    customer_data = generate_customer_data(
        customer_id=customer_id,
        state=state,
        postal=postal,
        date=statement_date,
        num_transactions=27,
    )
    file_name = generate_pdf(PDF_PATH, customer_data)

    # assertions
    assert os.path.exists(file_name)
    os.remove(file_name)
