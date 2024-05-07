import asyncio
import random
import os
import time
from datetime import date
from dateutil.relativedelta import relativedelta

from gen_sample_data.generate_data import (
    generate_customer_data_async,
    load_us_population_data_async,
    generate_pdf_async,
    remove_pdf_async,
)
from box_utils.box_client_ccg import ConfigCCG, get_ccg_user_client
from box_utils.box_uploads import box_upload_file_async, box_upload_file

US_500 = "sample-data/us_pop_500.csv"


async def create_files():
    us_pop = await load_us_population_data_async(US_500)
    config = ConfigCCG()
    client = get_ccg_user_client(config, config.ccg_user_id)

    # for i in range(len(us_pop["State"])):
    for i in range(1):
        for n in range(int(us_pop["Customers"][i])):

            today = date.today()  # Get today's date
            statement_date = date(today.year, today.month, 1)
            customer_start = time.perf_counter()
            for d in range(51):  # Months
                file_time_start = time.perf_counter()

                customer_data = await generate_customer_data_async(
                    customer_id=f"{us_pop['Postal'][i]}-{n+1+100000}",
                    state=us_pop["State"][i],
                    postal=us_pop["Postal"][i],
                    date=statement_date,
                    num_transactions=random.randint(5, 27),
                )
                statement = await generate_pdf_async(customer_data)

                # upload file to box
                box_file = await box_upload_file_async(client, config.folder_id, statement)
                # box_file = upload_file(client, config.folder_id, statement)

                # add metadata to file

                # handle limits
                # recover from errors

                # remove file from local folder
                await remove_pdf_async(statement)

                file_time_elapsed = time.perf_counter() - file_time_start
                print(
                    f"File {d+1} for {us_pop['State'][i]}-{us_pop['Postal'][i]} created in {file_time_elapsed:0.4f} seconds"
                )
                # Previous month
                statement_date = statement_date - relativedelta(months=1)

            print(
                f"Customer {us_pop['State'][i]}-{us_pop['Postal'][i]}-{n+1+100000} created in {time.perf_counter() - customer_start:0.4f} seconds"
            )


async def main():
    await create_files()


if __name__ == "__main__":
    asyncio.run(main())
