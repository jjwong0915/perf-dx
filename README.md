# Performance Diagnosis with Logs and Metrics
This project introduced two approaches on analyzing performance bugs for system logs and metrics: keyword-based and two-indicator. Keyword-based approach ranks logs based on the weighting of automatically selected keywords. Two-indicator approach utilizes the hotness of timing and the uniqueness of wording of logs to rank them.

## Code Structure
* `reproduce`: This folder contains scripts and results of performance bug reproduces.
    + `manual`: The experiments with manual triggering of bug and collecting of data.
    + `automated`: Fully automated bug reproduce with log and metric collection.
* `diagnosis`: The implementation and experiment result of both approaches.
    + `v1`: For keyword-based approach.
    + `v2`: For two-indicator approach.

## Example Usage
1. Create Python vitural environment and install dependencies
    ```
    $ cd perf-dx
    $ python -m venv .venv
    $ .venv/bin/pip install -r requirements.txt
    ```

2. To run the two-indicator log analysis, you have to set the environment variable
`RECORD_ROOT` as the absolute path to the folder contains bug reproduce results.
    ```
    $ export RECORD_ROOT=/home/myuser/perf-dx/reproduce/HADOOP-11252
    $ .venv/bin/python diagnosis/v2/main.py /tmp/result.csv
    ```

3. The basic result of analysis will be print on the standard output and 
   detials of uniquness and hotness can be found in `/tmp/result.csv` in this case.

