import contextlib
import csv
import datetime
import io
import logging
import multiprocessing
import os
import pathlib
import psutil
import signal
import subprocess
import sys
import time

LOGGER = logging.getLogger(__name__)
HADOOP_HOME = os.environ["HADOOP_HOME"]

def get_process(name):
    for p in psutil.process_iter(["cmdline"]):
        cmdline = p.info["cmdline"]
        if len(cmdline) >= 2 and cmdline[1] == f"-Dproc_{name}":
            return p
    raise RuntimeError("unable to find {name} process")

def collect_metric(node_type, pid=None):
    try:
        with open(f"{pathlib.Path(__file__).parent}/metric/{node_type}.csv", "w", newline="") as output_file:
            writer = csv.writer(output_file)
            process = get_process(node_type) if pid == None else psutil.Process(pid)
            process.cpu_percent()
            time.sleep(0.1)
            #
            start_time = time.monotonic_ns()
            while True:
                writer.writerow([
                    datetime.datetime.now().isoformat(), 
                    process.cpu_percent(),
                    process.memory_percent("uss"),
                ])
                output_file.flush()
                # wait until next second
                time.sleep((pow(10, 9) - ((time.monotonic_ns() - start_time) % pow(10, 9))) / pow(10, 9))
    except:
        LOGGER.exception("error occured while collecting metric")

def collect_server_logs(node_type):
    try:
        with contextlib.ExitStack() as stack:
            input_path = f"{HADOOP_HOME}/logs/hadoop-{os.getlogin()}-{node_type}-{os.uname()[1]}.log"
            input = stack.enter_context(open(input_path))
            input.seek(0, io.SEEK_END)
            print("start collecting logs from", str(input_path))
            output = stack.enter_context(
                open(f"{pathlib.Path(__file__).parent}/log/{node_type}.log", "w")
            )
            while True:
                while True:
                    line = input.readline()
                    if not line:
                        break
                    output.write(line)
                output.flush()
                time.sleep(0.25)
    except:
        LOGGER.exception("error occured while collecting logs")

def main():
    # clear local records
    parent = pathlib.Path(__file__).parent
    subprocess.run(["rm", "-rf", f"{parent}/log", f"{parent}/metric"])
    subprocess.run(["mkdir", f"{parent}/log", f"{parent}/metric"])
    # clear Hadoop logs
    subprocess.run(f"rm {HADOOP_HOME}/logs/*.log", shell=True)
    subprocess.run(["rm", "-rf", f"/tmp/hadoop-{os.getlogin()}"])
    subprocess.run([f"{HADOOP_HOME}/bin/hdfs", "namenode", "-format", "-force"])
    # run terasort
    try:
        subprocess.run([f"{HADOOP_HOME}/sbin/start-dfs.sh"])
        subprocess.run([
            f"{HADOOP_HOME}/bin/hadoop",
            "jar",
            f"{HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.5.0.jar",
            "teragen",
            "10000000",
            "/input",
        ], stdout=sys.stdout, stderr=sys.stderr)
        #
        with contextlib.ExitStack() as stack:
            terasort = stack.enter_context(subprocess.Popen([
                f"{HADOOP_HOME}/bin/hadoop",
                "jar",
                f"{HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.5.0.jar",
                "terasort",
                "/input",
                "/output",
            ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT))
            #
            pool = stack.enter_context(multiprocessing.Pool(5))
            pool.apply_async(collect_metric, ["namenode"])
            pool.apply_async(collect_metric, ["datanode"])
            pool.apply_async(collect_metric, ["client", terasort.pid])
            pool.apply_async(collect_server_logs, ["namenode"])
            pool.apply_async(collect_server_logs, ["datanode"])
            stack.enter_context(subprocess.Popen(
                ["tee", f"{parent}/log/client.log"],
                stdin=terasort.stdout,
                stdout=sys.stdout,
            ))
            # simulate server hang
            time.sleep(30)
            namenode = get_process("namenode")
            namenode.send_signal(signal.SIGSTOP)
            print(f"SIGSTOP has been send to namenode process")
            #
            time.sleep(30)
            namenode.send_signal(signal.SIGCONT)
            print(f"SIGCONT has been send to namenode process")
    finally:
        subprocess.run([f"{HADOOP_HOME}/sbin/stop-dfs.sh"])

main()
