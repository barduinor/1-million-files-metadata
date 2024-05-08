import csv
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import random
from reportlab.pdfgen import canvas
import os


def load_us_population_data(
    filename: str = "sample-data/us_pop_500.csv",
) -> dict:
    """
    Loads a CSV file into a dictionary (manual approach).

    Args:
        filename: The path to the CSV file.

    Returns:
        A dictionary where keys are column names (from first row)
        and values are lists of corresponding values.
    """
    with open(filename, "r") as csv_file:
        reader = csv.reader(csv_file)
        headers = next(reader)  # Read the first row as headers
        data = list(reader)
        return {h: [row[i] for row in data] for i, h in enumerate(headers)}


async def load_us_population_data_async(filename: str = "sample-data/us_pop_500.csv") -> dict:
    return load_us_population_data(filename)


def generate_customer_data(
    customer_id: str,
    state: str,
    postal: str,
    date: date,
    num_transactions: int = 3,
) -> dict:

    # max num transactions is 27
    if num_transactions > 27:
        num_transactions = 27

    total = 0
    transactions = []
    for i in range(num_transactions):

        transaction_id = f"{customer_id}-{i+1}"
        amount = round(random.uniform(10, 100), 2)

        transaction = {
            "Transaction ID": transaction_id,
            "Amount": amount,
        }
        total += amount
        transactions.append(transaction)

    statement = {
        "CustomerID": customer_id,
        "State": state,
        "Postal": postal,
        "Date": date,
        "Transactions": transactions,
        "Total": round(total,2),
    }

    return statement


async def generate_customer_data_async(
    customer_id: str,
    state: str,
    postal: str,
    date: date,
    num_transactions: int = 3,
) -> dict:
    return generate_customer_data(customer_id, state, postal, date, num_transactions)


def generate_pdf(folder_path:str, customer_data: dict) -> str:
    # Generate a PDF file based on the customer data

    file_name = (
        f"{folder_path}{customer_data["CustomerID"]}-{customer_data["Date"]}-{len(customer_data["Transactions"])}-{int(customer_data['Total']*100)}.pdf"
    )

    pdf = canvas.Canvas(file_name)

    # Set font and size
    pdf.setFont("Helvetica", 12)

    # Add Customer Information
    pdf.drawString(100, 700, f"CustomerID: {customer_data['CustomerID']}")
    pdf.drawString(100, 680, f"State: {customer_data['State']} ({customer_data['Postal']})")
    pdf.drawString(100, 640, f"Statement Date: {customer_data['Date']}")

    # Add Transaction Header
    pdf.drawString(100, 600, "Date")
    pdf.drawString(200, 600, "Transaction ID")
    pdf.drawString(350, 600, "Amount")

    # Set the statement date converting to proper date
    statement_date = customer_data["Date"]
    # set date to first date of previous month
    statement_date = statement_date - relativedelta(months=1)

    # Add Transactions
    y_pos = 580
    for transaction in customer_data["Transactions"]:
        pdf.drawString(100, y_pos, statement_date.strftime("%Y-%m-%d"))
        pdf.drawString(200, y_pos, transaction["Transaction ID"])
        pdf.drawString(350, y_pos, f"${transaction['Amount']:.2f}")
        y_pos -= 20
        statement_date = statement_date + timedelta(days=1)

    # Add Total
    y_pos -= 20
    pdf.drawString(100, y_pos, "Total:")
    pdf.drawString(350, y_pos, f"${customer_data['Total']:.2f}")

    # Save the PDF
    pdf.save()

    return file_name


async def generate_pdf_async(folder_path:str,customer_data: dict) -> str:
    return generate_pdf(folder_path,customer_data)


