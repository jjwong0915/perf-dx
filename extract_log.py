import collections
import datetime
import numpy as np
import pathlib
import re
import sys
import time
from gensim import downloader
from sklearn import cluster

def split_time(line): 
    try:
        timestamp = int(time.mktime(time.strptime(line[:19], "%Y-%m-%d %H:%M:%S")))
        return timestamp, line[24:]
    except (IndexError, ValueError):
        return None, line

def pooling_func(arr, axis=1):
    return np.transpose(arr)[np.argmax(np.sum(np.square(arr), axis=0))]

def main():
    print("building metric list")
    metric_map = collections.defaultdict(lambda: np.array([0.0, 0]))
    metric_begin = 0
    metric_end = 2147483647
    for metric_path in pathlib.Path("experiment", sys.argv[1]).glob("*.txt"):
        print("found metric:", metric_path)
        with metric_path.open() as metric_file:
            for line in metric_file:
                time_str, cpu_str, mem_str = line.split()
                timestamp = int(datetime.datetime.fromisoformat(time_str).timestamp())
                metric_map[timestamp] += np.array([float(cpu_str), int(mem_str)])
                metric_begin = min(timestamp, metric_begin)
                metric_end = max(timestamp, metric_end)
    #
    print("building log list")
    log_list = []
    for log_path in pathlib.Path("experiment", sys.argv[1]).glob("*.log"):
        print("found log:", log_path)
        with log_path.open() as log_file:
            for line in log_file:
                timestamp, content = split_time(line)
                if timestamp == None and len(log_list) > 0:
                    timestamp = log_list[-1][0]
                if timestamp <= metric_begin or timestamp > metric_end:
                    continue
                log_list.append((timestamp, content.strip("\n")))
    #
    print("loading word2vec")
    word2vec = downloader.load("glove-wiki-gigaword-50")
    #
    print("building keyword mapping")
    log_map = {}
    for timestamp, content in log_list:
        line_matrix = []
        line_map = {}
        for word in re.split("[^a-zA-Z0-9]+", content):
            try:
                if len(word) > 0:
                    vector = word2vec[word.lower()]
                    line_matrix.append(vector)
                    line_map[tuple(vector)] = word
            except KeyError:
                pass
        while len(line_matrix) < 5:
            line_matrix.append(np.zeros(50))
            line_map[tuple(np.zeros(50))] = ""
        #
        agglo = cluster.FeatureAgglomeration(n_clusters=5, pooling_func=pooling_func)
        log_feature = np.transpose(agglo.fit_transform(np.transpose(line_matrix)))
        log_map[content] = [line_map[tuple(vec)] for vec in log_feature]
    #
    print("building value mapping")
    word_map = collections.defaultdict(lambda: 0)
    for timestamp, content in log_list:
        for key in log_map[content]:
            word_map[key] += abs(metric_map[timestamp] - metric_map[timestamp-1])
    #
    print("calculating log values")
    log_val = []
    for timestamp, content in log_list:
        val = 0
        for key in log_map[content]:
            val += word_map[key]
        log_val.append((val, content))
    #
    print("writing results to cpu_order.txt and mem_order.txt")
    cpu_order = sorted(log_val, key=lambda x: x[0][0])
    with open("cpu_order.txt", mode="w") as cpu_file:
        for val, content in cpu_order:
            print(val, content, file=cpu_file)
    mem_order = sorted(log_val, key=lambda x: x[0][1])
    with open("mem_order.txt", mode="w") as mem_file:
        for val, content in mem_order:
            print(val, content, file=mem_file)


if __name__ == "__main__":
    main()
