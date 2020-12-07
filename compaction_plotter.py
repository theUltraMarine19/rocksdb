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
    
    job_to_level, job_to_time = {}, {}
    for compaction in d:
        job_to_level[compaction] = d[compaction][1]["output_level"]
        job_to_time[compaction] = d[compaction][1]["time_micros"]
        
    # for compaction in d:
    #     print(d[compaction])
    #     print("\n\n")

    level_wise_comps, level_wise_counts, times, y = {}, {}, [], []
    count, start_time, first = 0, 0, True
    for level in range(7):
        level_wise_comps[level] = {"times":[], "count":[], "compaction_time":[], "cpu_time":[], 
        "num_output_files":[], "output_size":[], "input_records":[], "output_records":[], "compacted_to_times":[],
        "write_rate":[]}
        level_wise_counts[level] = 0
    for line in lines:
        if "compaction_started" in line:
            index = line.find("{")
            line = line[index:]
            line = json.loads(line)
            time, level = line["time_micros"], job_to_level[line["job"]]
            if first:
                start_time = time
                first = False
            level_wise_counts[level] += 1
            level_wise_comps[level]["times"].append((time-start_time)/1000000)
            level_wise_comps[level]["count"].append(level_wise_counts[level])
            level_wise_comps[level]["compaction_time"].append(None)
            level_wise_comps[level]["cpu_time"].append(None)
            level_wise_comps[level]["num_output_files"].append(None)
            level_wise_comps[level]["output_size"].append(None)
            level_wise_comps[level]["input_records"].append(None)
            level_wise_comps[level]["output_records"].append(None)
        if "compaction_finished" in line:
            index = line.find("{")
            line = line[index:]
            line = json.loads(line)
            time, level = line["time_micros"], job_to_level[line["job"]]
            level_wise_counts[level] -= 1
            level_wise_comps[level]["times"].append((time-start_time)/1000000)
            level_wise_comps[level]["count"].append(level_wise_counts[level])
            level_wise_comps[level]["compaction_time"].append(line["compaction_time_micros"]/1000)
            level_wise_comps[level]["cpu_time"].append(line["compaction_time_cpu_micros"]/1000)
            level_wise_comps[level]["num_output_files"].append(line["num_output_files"])
            level_wise_comps[level]["output_size"].append(line["total_output_size"]/(1024*1024))
            level_wise_comps[level]["input_records"].append(line["num_input_records"])
            level_wise_comps[level]["output_records"].append(line["num_output_records"])
        if "compacted to" in line:
            index = line.find("[default]") + len("[default]") + 1 
            index1 = line.find(" ", index)
            compaction = int(line[index:index1])
            read_rate = float(line.find(" ", line.find("MB/sec:") + len("MB/sec:") + 1))
            write_rate = float(line.find(" ", line.find("rd,") + len("rd,") + 1))
    
    # print(level_wise_comps[0])
    # print()
    # print(level_wise_comps[1])
    # print()
    # print(level_wise_comps[2])
    # print(level_wise_comps)

def print_lines(lines):
    for line in lines:
        print(line)

if __name__ == "__main__":
    print("hello")
    # lines = read_file("/tmp/rocksdbtest-20001/dbbench/LOG")
    # lines = read_file("/mydata/rocksdb/LOG")
    lines = read_file("/users/nithinv/rocksdbLOGS/LOG")
    start_time = lines[0].split()[0]
    
    # start_time = time.mktime(datetime.datetime.strptime(start_time, "%Y/%m/%d").timetuple())
    start_time = (datetime.datetime.strptime(start_time, "%Y/%m/%d-%H:%M:%S.%f") - datetime.datetime(1970, 1, 1)).total_seconds()*1000000
    # lines = parse_data_for_compactions(lines, start_time)
    # print_lines(lines)
    parse_compaction_event(lines, start_time)