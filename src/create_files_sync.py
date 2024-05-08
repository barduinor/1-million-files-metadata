import asyncio
import aiofiles
import random
import os
import time
from datetime import date,datetime
from dateutil.relativedelta import relativedelta

from gen_sample_data.generate_data import (
    generate_customer_data,
    load_us_population_data,
    generate_pdf,
)
from gen_sample_data.generate_files import delete_file,log_last_process,append_log
from box_utils.box_client_ccg import ConfigCCG, get_ccg_user_client
from box_utils.box_uploads import box_upload_file_async, box_upload_file

DATA_DEFINITION = "sample-data/us_pop_500.csv"
# DATA_DEFINITION = "sample-data/20M Customers.csv"
PDF_PATH = "sample-data/files/"
LOG_PATH = f"sample-data/logs/"
LOG_FILE = f"{LOG_PATH}log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

def create_file(customer_id:str,state:str,postal:str,date:date,num_transactions:int,create_log:bool=False) -> float:
    file_time_start = time.perf_counter()

    customer_data = generate_customer_data(
        customer_id=customer_id,
        state=state,
        postal=postal,
        date=date,
        num_transactions=num_transactions,
    )
    statement = generate_pdf(PDF_PATH,customer_data)

    if create_log:
        append_log(
            LOG_FILE, 
            f'"{statement}","{customer_data['CustomerID']}","{customer_data['State']}","{customer_data['Postal']}",{customer_data['Date']},{num_transactions},{customer_data['Total']}')
    
    file_time_elapsed = time.perf_counter() - file_time_start

    return file_time_elapsed



def create_files(worker_name:str="worker-0",recover_path:str=LOG_PATH, create_log:bool=False):
    us_pop = load_us_population_data(DATA_DEFINITION)
    # config = ConfigCCG()
    # client = get_ccg_user_client(config, config.ccg_user_id)

    # make sure paths exist
    if not os.path.exists(LOG_PATH):
        os.makedirs(LOG_PATH)

    if not os.path.exists(PDF_PATH):
        os.makedirs(PDF_PATH)

    recover_log_file = f"{LOG_PATH}{worker_name}-recover.txt"
    if os.path.exists(recover_log_file):
        with open(recover_log_file, mode="r") as f:
            last_process = f.readline()
            state_offset, customer_offset, date_offset = last_process.split(",")
            state_offset = int(state_offset)
            customer_offset = int(customer_offset)
            date_offset = int(date_offset)
    else:
        state_offset = 0
        customer_offset = 0
        date_offset = 0

    batch_start = time.perf_counter()
    for state_index in range(state_offset,len(us_pop["State"])):
        state_start = time.perf_counter()  
        for customer_index in range(customer_offset,int(us_pop["Customers"][state_index])):

            today = date.today()  # Get today's date
            statement_date = date(today.year, today.month, 1)
            if date_offset > 0:
                statement_date = statement_date - relativedelta(months=date_offset)
            # customer_start = time.perf_counter()
            for date_index in range(date_offset,50):  # Months
                create_file(
                    f"{us_pop['Postal'][state_index]}-{customer_index+1:07}", 
                    us_pop["State"][state_index], 
                    us_pop["Postal"][state_index], 
                    statement_date, 
                    random.randint(5, 27)
                )
                log_last_process(recover_log_file, f"{state_index},{customer_index},{date_index}")

                statement_date = statement_date - relativedelta(months=1)
            date_offset = 0
        print(
                f"State {us_pop["State"][state_index]} created in {time.perf_counter() - state_start:0.3f} seconds"
            )
        customer_offset = 0
    
    print(f"Batch created in {time.perf_counter() - batch_start:0.3f} seconds")


def main():
    create_files()


if __name__ == "__main__":
    main()
