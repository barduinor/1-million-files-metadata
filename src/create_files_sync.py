import asyncio
import aiofiles
import random
import os
import time
from datetime import date,datetime
from dateutil.relativedelta import relativedelta
import sys

from gen_sample_data.generate_data import (
    Workload,
    generate_customer_data,
    load_us_population_data,
    generate_pdf,
)
from gen_sample_data.generate_files import delete_file,log_last_process,append_log
from box_utils.box_client_ccg import ConfigCCG, get_ccg_user_client
from box_utils.box_uploads import box_upload_file_async, box_upload_file

def create_file(
    worker_name:str,
    pdf_path:str,
    log_path:str,
    customer_id:str,
    state:str,
    postal:str,
    date:date,
    num_transactions:int,
    create_metadata:bool=False
) -> float:

    file_time_start = time.perf_counter()

    customer_data = generate_customer_data(
        customer_id=customer_id,
        state=state,
        postal=postal,
        date=date,
        num_transactions=num_transactions,
    )
    statement = generate_pdf(f"{pdf_path}{worker_name}",customer_data)

    if create_metadata:
        log_file_name = f"{log_path}{worker_name}_log.csv"
        append_log(
            log_file_name, 
            f'"{statement}","{customer_data['CustomerID']}","{customer_data['State']}","{customer_data['Postal']}",{customer_data['Date']},{num_transactions},{customer_data['Total']}')
    
    file_time_elapsed = time.perf_counter() - file_time_start

    return file_time_elapsed



def create_files(
    workload:list[Workload],
    worker_name:str="worker-0",
    pdf_path:str="sample-data/files/",
    log_path:str="sample-data/logs/", 
    number_of_statements:int=50,
    auto_remove_files:bool=False,
    create_metadata:bool=False
    ):

    # make sure paths exist
    if not os.path.exists(log_path):
        os.makedirs(log_path)

    if not os.path.exists(f"{pdf_path}{worker_name}"):
        os.makedirs(f"{pdf_path}{worker_name}")

    recover_log_file = f"{log_path}{worker_name}-recover.txt"
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
    files_in_batch =0
    for state_index in range(state_offset,len(workload)):
        files_in_state = 0
        state_start = time.perf_counter()  
        for customer_index in range(workload[state_index].customers_start+customer_offset,workload[state_index].customers_end):

            today = date.today()  # Get today's date
            statement_date = date(today.year, today.month, 1)
            if date_offset > 0:
                statement_date = statement_date - relativedelta(months=date_offset)
        
            # customer_start = time.perf_counter()
            for date_index in range(date_offset,number_of_statements):  # Months
                create_file(
                    worker_name,
                    pdf_path,
                    log_path,
                    f"{workload[state_index].postal}-{customer_index+1:07}", 
                    workload[state_index].state, 
                    workload[state_index].postal, 
                    statement_date, 
                    random.randint(5, 27),
                    create_metadata,
                )
                log_last_process(recover_log_file, f"{state_index},{customer_index},{date_index}")
                files_in_state += 1
                files_in_batch += 1
                statement_date = statement_date - relativedelta(months=1)
            date_offset = 0
        elapsed_time = time.perf_counter() - state_start
        files_per_second = files_in_state / elapsed_time
        print(
                f"{worker_name},file,{workload[state_index].state},{files_in_state},{elapsed_time:0.3f},{files_per_second:0.3f}"
            )
        customer_offset = 0
    elapsed_time = time.perf_counter() - batch_start
    files_per_second = files_in_batch / elapsed_time
    print(f"{worker_name},batch,ZZ,{files_in_batch},{elapsed_time:0.3f},{files_per_second:0.3f}")


def main():
    
    if len(sys.argv) > 1:
        DATA_DEFINITION = sys.argv[1]
        worker_name = sys.argv[2]
    else:
        DATA_DEFINITION = "sample-data/500 Customers.csv"
        # DATA_DEFINITION = "sample-data/2K Customers.csv"
        # DATA_DEFINITION = "sample-data/20M Customers.csv"

        worker_name = "worker-0"

    workload = load_us_population_data(DATA_DEFINITION)
    print(f"Starting {worker_name} using {DATA_DEFINITION}")
    create_files(workload,worker_name)


if __name__ == "__main__":
    main()
