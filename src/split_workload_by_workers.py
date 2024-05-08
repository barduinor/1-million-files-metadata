import os

from gen_sample_data.generate_data import (
    Workload,
    load_us_population_data,
    write_worker_files,
    write_execution_script,
)

WORKERS = 8


def split_workload_by_workers(
    workload: list[Workload], workers: int = WORKERS, worker_path: str = "sample-data/workers_load/"
) -> None:

    # make sure path exists
    if not os.path.exists(worker_path):
        os.makedirs(worker_path)
    total_customers = sum((item.customers_end - item.customers_start) for item in workload)
    customers_per_worker = (total_customers // workers) + 1

    print(
        f"Total customers: {total_customers}, {WORKERS} workers: {total_customers // WORKERS} customers per worker"
    )

    workers_load: list[list[Workload]] = []
    worker_load: list[Workload] = []

    for state in workload:
        state_customers = state.customers_end - state.customers_start
        worker_load_customers = sum((item.customers_end - item.customers_start) for item in worker_load)
        customers_available = customers_per_worker - worker_load_customers

        if customers_available <= 0:
            workers_load.append(worker_load)
            worker_load = []
            worker_load.append(state)
            continue

        if state_customers <= customers_available:
            worker_load.append(state)
            # if it is the last state , add to the workers_load
            if state == workload[-1]:
                workers_load.append(worker_load)

        else:
            worker_load.append(
                Workload(
                    state.postal,
                    state.state,
                    state.population,
                    state.population_percent,
                    state.customers_start,
                    state.customers_start + customers_available,
                )
            )
            workers_load.append(worker_load)
            worker_load = []
            worker_load.append(
                Workload(
                    state.postal,
                    state.state,
                    state.population,
                    state.population_percent,
                    state.customers_start + customers_available,
                    state.customers_end,
                )
            )

    tot_cust = 0
    for worker in workers_load:
        # sort the worker by state
        worker.sort(key=lambda x: x.postal)

        for state in worker:
            print(f"{state.state}: \t\t{state.customers_end-state.customers_start:,}")
            tot_cust += state.customers_end - state.customers_start

        print("----")
        print(f"Total: {sum((item.customers_end - item.customers_start) for item in worker)}")
        print()
    print(f"Total customers: {tot_cust:,}")
    # print total number of unique states

    tot_states = 0
    for i, worker in enumerate(workers_load):
        print(f"Worker {i}: {len(worker)} states")
        tot_states += len(worker)

    print(f"Total states: {tot_states}")

    write_worker_files(workers_load, worker_path)


def main():

    DATA_DEFINITION = "sample-data/500 Customers.csv"
    # DATA_DEFINITION = "sample-data/2K Customers.csv"
    # DATA_DEFINITION = "sample-data/20M Customers.csv"

    workload = load_us_population_data(DATA_DEFINITION)
    split_workload_by_workers(workload)
    write_execution_script(WORKERS, "execution_script.sh")


if __name__ == "__main__":
    main()
