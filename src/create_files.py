import asyncio
from gen_sample_data.generate_data import (
    generate_customer_data_async,
    load_us_population_data_async,
    generate_pdf_async,
)
import random
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

US_500 = "sample-data/us_pop_500.csv"


async def create_files():
    us_pop = await load_us_population_data_async(US_500)

    for i in range(len(us_pop["State"])):
        # for i in range(2):
        for n in range(int(us_pop["Customers"][i])):
            today = date.today()  # Get today's date
            statement_date = date(today.year, today.month, 1)

            for d in range(51):  # Months

                customer_data = await generate_customer_data_async(
                    customer_id=f"{us_pop['Postal'][i]}-{n+1}",
                    state=us_pop["State"][i],
                    postal=us_pop["Postal"][i],
                    date=statement_date,
                    num_transactions=random.randint(1, 27),
                )
                statement = await generate_pdf_async(customer_data)

                # upload file to box
                # add metadata to file
                # remove file from local folder
                statement_date = statement_date - relativedelta(months=1)


async def main():
    await create_files()


if __name__ == "__main__":
    asyncio.run(main())
