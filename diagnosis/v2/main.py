import collections
import csv
import datetime
import os
import pathlib
import re
import statistics
import sys

RECORD_ROOT = pathlib.Path(os.getenv("RECORD_ROOT"))
TIME_FORMATS = [
    ("\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d", "%Y-%m-%d %H:%M:%S"), 
    ("\d\d/\d\d/\d\d \d\d:\d\d:\d\d", "%y/%m/%d %H:%M:%S"),
    ("\d\d/\W\W\W/\d\d\d\d:\d\d:\d\d:\d\d", "%y/%b/%d %H:%M:%S"),
]
HOTSPOT_THRES = 3
HOTNESS_DIST = 3

def parse_time(line):
    for format in TIME_FORMATS:
        try:
            return datetime.datetime.strptime(re.findall(format[0], line)[0], format[1])
        except (IndexError, ValueError):
            pass
    return None

def main():
    # read metric files
    metric_table = {}
    for metric_path in RECORD_ROOT.glob("metric/*.csv"):
        metric_table[metric_path.stem] = []
        with metric_path.open(newline="") as csv_file:
            reader = csv.reader(csv_file)
            for row in reader:
                metric_table[metric_path.stem].append({
                    "time": datetime.datetime.fromisoformat(row[0]),
                    "cpu": float(row[1]),
                    "mem": float(row[2]),
                })

    # find metric hotspot
    hotspot_table = {}
    for key in metric_table.keys():
        cpu_diff = []
        mem_diff = []
        for idx in range(1, len(metric_table[key])):
            cpu_diff.append(abs(metric_table[key][idx]["cpu"] - metric_table[key][idx-1]["cpu"]))
            mem_diff.append(abs(metric_table[key][idx]["mem"] - metric_table[key][idx-1]["mem"]))
        #
        cpu_thres = statistics.mean(cpu_diff) + statistics.stdev(cpu_diff) * HOTSPOT_THRES
        mem_thres = statistics.mean(mem_diff) + statistics.stdev(mem_diff) * HOTSPOT_THRES
        hotspot_table[key] = []
        for idx in range(len(metric_table[key]) - 1):
            if cpu_diff[idx] > cpu_thres or mem_diff[idx] > mem_thres:
                hotspot_table[key].append(metric_table[key][1+idx]["time"])

    # read log files
    log_table = {}
    for key in metric_table.keys():
        log_table[key] = []
        with RECORD_ROOT.joinpath("log", f"{key}.log").open() as log:
            for line in log:
                time = parse_time(line)
                if time:
                    # only keep words in each line
                    word_set = list(set(re.findall("[a-zA-Z]+", line.lower())))
                    log_table[key].append({
                        "orig": line,
                        "time": time, 
                        "word": word_set,
                    })

    # sum word occurance
    occurance_table = {}
    for key in log_table.keys():
        occurance_table[key] = collections.defaultdict(lambda: 0)
        for log in  log_table[key]:
            for word in log["word"]:
                occurance_table[key][word] += 1

    # add uniqueness to log table
    for key in log_table.keys():
        for log in log_table[key]:
            log["uniq"] = statistics.mean(1/occurance_table[key][word] for word in log["word"])

    # calc log scores
    log_score = []
    for key in log_table.keys():
        for log in log_table[key]:
            if len(hotspot_table[key]) > 0:
                distance = min(abs(log["time"]-hs) for hs in hotspot_table[key])
                hotness = statistics.NormalDist(sigma=HOTNESS_DIST).pdf(distance.total_seconds())
            else:
                hotness = 1
            score = log["uniq"] * hotness
            log_score.append([score, {
                "orig": log["orig"],
                "uniq": log["uniq"],
                "hot": hotness,
            }])
    log_score.sort(key=lambda x: x[0], reverse=True)
    
    # print result
    print("total log count:", len(log_score))
    print("logs with top 10 score:")
    for log in log_score[:10]:
        print(f"{log[0]:.3f}  {re.sub('[^a-zA-Z]+', '', log[1]['orig'], count=1)[:110].strip()}")

    if len(sys.argv) >= 2:
        with open(sys.argv[1], "w", newline="") as output_file:
            writer = csv.writer(output_file)
            writer.writerow(["score", "hotness", "uniqueness", "log"])
            for log in log_score:
                writer.writerow([log[0], log[1]["hot"], log[1]["uniq"], log[1]["orig"].strip()])
        print(f"result is saved to {sys.argv[1]}")

main()
