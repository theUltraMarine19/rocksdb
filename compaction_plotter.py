# import matplotlib.pyplot as plt
import time
import datetime

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
    
def print_lines(lines):
    for line in lines:
        print(line)

if __name__ == "__main__":
    print("hello")
    lines = read_file("/tmp/rocksdbtest-20001/dbbench/LOG")
    start_time = lines[0].split()[0]
    
    # start_time = time.mktime(datetime.datetime.strptime(start_time, "%Y/%m/%d").timetuple())
    start_time = (datetime.datetime.strptime(start_time, "%Y/%m/%d-%H:%M:%S.%f") - datetime.datetime(1970, 1, 1)).total_seconds()
    lines = parse_data_for_compactions(lines, start_time)
    # print_lines(lines)