import asyncio
import aiofiles
import random
import os
import time
from datetime import date,datetime
from dateutil.relativedelta import relativedelta

from gen_sample_data.generate_data import (
    generate_customer_data_async,
    load_us_population_data_async,
    generate_pdf_async,

)
from box_utils.box_client_ccg import ConfigCCG, get_ccg_user_client
from box_utils.box_uploads import box_upload_file_async, box_upload_file

DATA_DEFINITION = "sample-data/us_pop_500.csv"
# DATA_DEFINITION = "sample-data/20M Customers.csv"
PDF_PATH = "sample-data/files/"
LOG_PATH = f"sample-data/logs/"
LOG_FILE = f"{LOG_PATH}log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

async def append_log(log_file, line):
    if not os.path.exists(LOG_PATH):
        os.makedirs(LOG_PATH)
    async with aiofiles.open(log_file, mode="a") as f:
        await f.write(line + "\n")

async def create_file(customer_id:str,state:str,postal:str,date:date,num_transactions:int) -> float:
    file_time_start = time.perf_counter()

    customer_data = await generate_customer_data_async(
        customer_id=customer_id,
        state=state,
        postal=postal,
        date=date,
        num_transactions=num_transactions,
    )
    statement = await generate_pdf_async(PDF_PATH,customer_data)

    await append_log(
        LOG_FILE, 
        f'"{statement}","{customer_data['CustomerID']}","{customer_data['State']}","{customer_data['Postal']}",{customer_data['Date']},{num_transactions},{customer_data['Total']}')
    
    file_time_elapsed = time.perf_counter() - file_time_start
    return file_time_elapsed



async def create_files():
    us_pop = await load_us_population_data_async(DATA_DEFINITION)
    config = ConfigCCG()
    # client = get_ccg_user_client(config, config.ccg_user_id)

    batch_start = time.perf_counter()
    for i in range(len(us_pop["State"])):
        state_start = time.perf_counter()  
        for n in range(int(us_pop["Customers"][i])):

            today = date.today()  # Get today's date
            statement_date = date(today.year, today.month, 1)
            customer_start = time.perf_counter()
            for d in range(51):  # Months
                await create_file(
                    f"{us_pop['Postal'][i]}-{n+1:07}", 
                    us_pop["State"][i], 
                    us_pop["Postal"][i], 
                    statement_date, 
                    random.randint(5, 27)
                )

                statement_date = statement_date - relativedelta(months=1)

        print(
                f"State {us_pop["State"][i]} created in {time.perf_counter() - state_start:0.3f} seconds"
            )
    print(f"Batch created in {time.perf_counter() - batch_start:0.3f} seconds")


async def main():
    await create_files()


if __name__ == "__main__":
    asyncio.run(main())
