# import matplotlib.pyplot as plt
from collections import defaultdict
import time
import datetime
import json

def read_file(file_name):
    with open(file_name, "r") as f:
        lines = list(f.read().split("\n"))
        return lines

def parse_data_for_compactions(lines, start_time):
    parsed_lines = []
    matches = ["Compaction start summary", "compaction_started", "compacted to"]

    for line in lines:
        if any(x in line for x in matches):
            parsed_lines.append(line)

    count = 0
    x, y = [], []
    for line in parsed_lines:
        if "Compaction start summary" in line:
            count += 1
            time = (datetime.datetime.strptime(line.split()[0], "%Y/%m/%d-%H:%M:%S.%f") - datetime.datetime(1970, 1, 1)).total_seconds()
            x.append(time-start_time)
            y.append(count)
        if "compacted to" in line:
            count -= 1

    print(x)
    print(y)
    
    # plt.plot(x, y)

def parse_compaction_event(lines, start_time):
    d = defaultdict(list)
    for line in lines:
        if any(x in line for x in ["compaction_started", "compaction_finished"]):
            index = line.find("{")
            line = line[index:]
            line = json.loads(line)
            d[line["job"]].append(line)
        
    # for compaction in d:
    #     print(d[compaction])
    #     print("\n\n")

    times = []
    y = []
    count, start_time, first = 0, 0, True
    for line in lines:
        if "compaction_started" in line:
            index = line.find("{")
            line = line[index:]
            line = json.loads(line)
            time = line["time_micros"] 
            count += 1
            if first:
                start_time = time
                first = False
            times.append((time-start_time)/1000000)
            y.append(count)
        if "compaction_finished" in line:
            index = line.find("{")
            line = line[index:]
            line = json.loads(line)
            time = line["time_micros"]
            count -= 1 
            times.append((time-start_time)/1000000)
            y.append(count)
    
    print(times)
    print(y)

    
def print_lines(lines):
    for line in lines:
        print(line)

if __name__ == "__main__":
    print("hello")
    lines = read_file("/tmp/rocksdbtest-20001/dbbench/LOG")
    # lines = read_file("/mydata/rocksdb/LOG")
    start_time = lines[0].split()[0]
    
    # start_time = time.mktime(datetime.datetime.strptime(start_time, "%Y/%m/%d").timetuple())
    start_time = (datetime.datetime.strptime(start_time, "%Y/%m/%d-%H:%M:%S.%f") - datetime.datetime(1970, 1, 1)).total_seconds()*1000000
    # lines = parse_data_for_compactions(lines, start_time)
    # print_lines(lines)
    parse_compaction_event(lines, start_time)