import pandas as pd
import argparse
import requests
import json
import os
import statistics
import numpy as np
from datetime import datetime


import random
random.seed(42)

import matplotlib.pyplot as plt
import matplotlib.patches as patches


# configs
COLD_START=False
SHOW_STAGE_IDS=True
SHOW_TASK_STAGE_IDS=True
EXECUTION_DATA="type" # group, type
EXECUTOR_AMOUNT = 8
CORES_PER_EXEC = 4

# result directories
RUN_PATH="./performance_test_2/performance_test/target"
BENCH_PATH=f"{RUN_PATH}/bench_outputs"
BASE_BENCH_PATH=f"{RUN_PATH}/bench_outputs/base"
APPS_URL="http://localhost:18080/api/v1/applications"




# Numerical constants
GIGA = 1000000000
MS_TO_S = 1 / 1000 
NS_TO_S = 1 / 1000000000

# drawing constants
JOBGROUP_BIN_SIZE=5
JOBGROUP_BIN_DIST=0.3
STAGE_DIST=0.1


SCHEDULERS = [
    "CUSTOM_FAIR",
    "CUSTOM_RANDOM",
    "FIFO",
    "FAIR",
]

CONFIGS = [
    "2_large_2_small_users",
    "4_large_users",
    "2_power_2_small_users"
]

PARTITIONS = [
    "coalesce",
    "repartition"
]


def get_human_name(filename):
    name = ""
    partition = "default"
    config = ""

    for sch in SCHEDULERS:
        if sch in filename:
            name = sch 
            break
    for conf in CONFIGS:
        if conf in filename:
            config = conf
            break

    for part in PARTITIONS:
        if part in filename:
            partition = part
            break


    return name, partition, config

def get_worst_10_percent(arr):
    arr = np.array(arr)
    n = max(1, int(len(arr) * 0.1))
    top_10_percent = np.partition(arr, -n)[-n:]
    return np.mean(top_10_percent)

class Benchmark:
    def __init__(self, scheduler, partitioning, config, users):
        self.scheduler = scheduler
        self.partitioning = partitioning
        self.config = config
        self.app_name = f"{scheduler}_{config}_{''  if partitioning == 'default'  else partitioning}"
        self.users = users
        self.get_event_data()



    def get_event_data(self):
        response = requests.get(APPS_URL)

        if response.status_code != 200:  # Check if the request was successful
            raise Exception(f"Error requesting eventlog data for application: {response.status_code}")
        # find the jobs for data
        data = response.json()  # Convert response to JSON
        app_id = next((entry["id"] for entry in data if self.app_name in entry["name"]), None)
        
        # get executor details

        response = requests.get(f"{APPS_URL}/{app_id}/executors")
        if response.status_code != 200:
            raise Exception(f"Error requesting eventlog data for executors: {response.status_code}")
        data = response.json()


        total_shuffle_read = 0
        total_shuffle_write = 0
        peak_JVM_memory = 0
        for executor in data:
            if executor["id"] == "driver":
                continue
            total_shuffle_read += executor["totalShuffleRead"]
            total_shuffle_write += executor["totalShuffleWrite"]
            used_peak_memory = executor["peakMemoryMetrics"]["JVMHeapMemory"] + executor["peakMemoryMetrics"]["JVMOffHeapMemory"]
            peak_JVM_memory = max(peak_JVM_memory, used_peak_memory)
            
            
        self.total_shuffle_read = total_shuffle_read / GIGA
        self.total_shuffle_write = total_shuffle_write / GIGA
        self.peak_JVM_memory = peak_JVM_memory / GIGA


        # get user and more detailed execution data 

        for user in self.users:
            user.get_event_data(app_id)





class Metrics:
    def __init__(self, name, is_seq, total_times_s, start_time_s, end_time):
        self.name = name
        self.is_seq = is_seq
        self.total_times = total_times_s
        self.start_time = start_time_s
        self.end_time = end_time
        if is_seq and COLD_START:
            self.mean = statistics.mean(total_times_s[1:])
            self.var = statistics.variance(total_times_s[1:])
        else:
            self.mean = statistics.mean(total_times_s)
            self.var = statistics.variance(total_times_s)
        

class Stage:
    def __init__(self,stage_id, start_s, end_ms):
        self.id = stage_id 
        self.start = start_s 
        self.end = end_ms
class Execution:
    def __init__(self, executor_id, ex_type, start_s, end_s, stage_id):
        self.executor_id = executor_id
        self.ex_type = ex_type 
        self.start = start_s 
        self.end = end_s
        self.stage_id = stage_id


class JobGroup:
    def __init__(self, name, jobs):
        self.name = name
        self.jobs = jobs


    def get_event_data(self, app_id):

        self.stages = []
        self.executor_load = []
        self.start = None
        self.end = None
        for job_id in self.jobs:
            response = requests.get(f"{APPS_URL}/{app_id}/jobs/{job_id}")
            if response.status_code != 200:
                raise Exception(f"Error requesting eventlog data for job {job_id}: {response.status_code}")

            job_json = response.json()

            # convert times to seconds since epoch
            job_start = job_json["submissionTime"]
            job_start_dt = datetime.strptime(job_start[:-3], "%Y-%m-%dT%H:%M:%S.%f")
            job_start_s= job_start_dt.timestamp()

            job_end = job_json["completionTime"]
            job_end_dt = datetime.strptime(job_end[:-3], "%Y-%m-%dT%H:%M:%S.%f")
            job_end_s= job_end_dt.timestamp()

            # find earliest and latest start and end time
            if self.start == None or self.start > job_start_s:
                self.start = job_start_s

            if self.end == None or self.end < job_end_s:
                self.end = job_end_s

            # get stages
            for stage_id in job_json["stageIds"]:
                response = requests.get(f"{APPS_URL}/{app_id}/stages/{stage_id}")
                if response.status_code != 200:
                    raise Exception(f"Error requesting eventlog data for stage {stage_id}: {response.status_code}")
                # returns an array of one element
                stage_json = response.json()[0]
                if stage_json["status"] == "COMPLETE":
                    # convert times to seconds since epoch
                    stage_start = stage_json["firstTaskLaunchedTime"]
                    stage_start_dt = datetime.strptime(stage_start[:-3], "%Y-%m-%dT%H:%M:%S.%f")
                    stage_start_s= stage_start_dt.timestamp()

                    stage_end = stage_json["completionTime"]
                    stage_end_dt = datetime.strptime(stage_end[:-3], "%Y-%m-%dT%H:%M:%S.%f")
                    stage_end_s= stage_end_dt.timestamp()
                    self.stages.append(Stage(stage_id, stage_start_s, stage_end_s))

                    # get how much time spent on executor
                    for task_key in stage_json["tasks"]:
                        task = stage_json["tasks"][task_key]
                        task_start = task["launchTime"]
                        task_start_dt = datetime.strptime(task_start[:-3], "%Y-%m-%dT%H:%M:%S.%f")
                        task_start_s =task_start_dt.timestamp()
                        executor_id = int(task["executorId"])

                        # assume task execution is first (its not, but other times are usually negligable)
                        task_metrics = task["taskMetrics"]
                        execution_time_s = task_metrics["executorRunTime"] * MS_TO_S
                        execution_start_s = task_start_s
                        execution_end_s = task_start_s + execution_time_s 
                        self.executor_load.append(Execution(executor_id, "EXEC", execution_start_s, execution_end_s, stage_id))

                        # write follows after execution
                        write_time_s = task_metrics["shuffleWriteMetrics"]["writeTime"] * NS_TO_S
                        write_start_s = execution_end_s 
                        write_end_s = write_start_s + write_time_s 
                        self.executor_load.append(Execution(executor_id, "WRITE", write_start_s, write_end_s, stage_id))


                        




def find_closest_to_0(nums):
    candidate = 0 
    while candidate in nums:
        candidate+=1 
    return candidate 

class Bin:
    def __init__(self, start, end, max=1, id=0):
        self.start = start 
        self.end = end 
        self.max = max
        self.id = id
        self.pos = 0
        self.subbins = []

    def add(self, bin_elem):

        overlap, pos = self.count_overlap(bin_elem)
        # set element to non overlapping position
        bin_elem.pos = pos
        self.max = max(self.max, overlap+ 1)
        self.subbins.append(bin_elem)

    def count_overlap(self, other):
        count = 0
        taken_pos = []
        for bin in self.subbins:
            if ((bin.start > other.start and bin.start < other.end) 
                or (bin.end > other.start and bin.end < other.end) 
                or (bin.start < other.start and bin.end > other.end)):
                    count += bin.max
                    taken_pos.append(bin.pos)
        
        pos = find_closest_to_0(taken_pos)
        return count, pos



    

class User:
    def __init__(self, name, workload_metrics, base_metrics):
        self.name = name
        self.workload_metrics = workload_metrics
        self.base_metrics = base_metrics
        self.slowdowns, self.proportional_slowdowns = self.calculate_slowdowns()



    def calculate_slowdowns(self):
        slowdowns = []
        proportional_slowdowns = []
        for metrics in self.workload_metrics:
            base = next((m for m in self.base_metrics if m.name == metrics.name))
            if metrics.is_seq and COLD_START:
                slowdown_metric = [metrics.total_times[0] - base.total_times[0]] + [m - base.mean for m in metrics.total_times[1:]]
                proportional_slowdown = [slowdown_metric[0] / base.total_times[0]] + [m / base.mean for m in slowdown_metric]
                slowdowns.append(slowdown_metric)
                proportional_slowdowns.append(proportional_slowdown)
            else:
                slowdown_metric = [m - base.mean for m in metrics.total_times]
                proportional_slowdown = [m / base.mean for m in slowdown_metric]
                slowdowns.append(slowdown_metric)
                proportional_slowdowns.append(proportional_slowdown)
        return slowdowns, proportional_slowdowns


    def get_event_data(self, app_id):
        response = requests.get(f"{APPS_URL}/{app_id}/jobs")
        if response.status_code == 200:  # Check if the request was successful
            # find the jobs for data
            jobs_json = response.json()  # Convert response to JSON

            user_jobgroup_map = {}
            for job in jobs_json:
                jobgroup = job["jobGroup"]
                if self.name in jobgroup:
                    # if first unique jobgroup, create list
                    if user_jobgroup_map.get(jobgroup) is None:
                        user_jobgroup_map[jobgroup] = []
                    
                    user_jobgroup_map[jobgroup].append(job["jobId"])
            
            self.jobgroups = []
            for jobgroup_key in user_jobgroup_map:
                jobs = user_jobgroup_map[jobgroup_key]
                jobgroup = JobGroup(jobgroup_key, jobs)
                # get all jobgroup event data and then append
                jobgroup.get_event_data(app_id)
                self.jobgroups.append(jobgroup)
                




        else:
            raise Exception(f"Error requesting eventlog data for jobs: {response.status_code}")










def get_workload_metrics(workload):
    results = workload["results"]
    total_times_s = []
    earliest_start_time = None
    latest_end_time = None
    for i in results:
        start_time_s = results[i]["start_time-setup_time"] * MS_TO_S
        end_time_s = results[i]["end_time-execution_time"] * MS_TO_S

        if earliest_start_time == None or earliest_start_time > start_time_s:
            earliest_start_time = start_time_s
        if latest_end_time == None or latest_end_time < end_time_s:
            latest_end_time = end_time_s

        start_up = results[i]["setup_time"] * MS_TO_S
        partition = results[i]["partitioning_time"] * MS_TO_S
        exec = results[i]["execution_time"] * MS_TO_S
        total_times_s.append(start_up + partition + exec)
        


    name = workload["className"]
    seq = workload["frequency"] == "SEQ"
    metrics = Metrics(name, seq, total_times_s, earliest_start_time, latest_end_time)
    return metrics

def get_bench_base(bench_path):
    with open(bench_path, 'r') as file:
        data = json.load(file)
        user = data["users"][0]
        workload = user["workloads"][0]
        metrics = get_workload_metrics(workload)
        return metrics
            





def get_bench_users(bench_path, base): 
    with open(bench_path, 'r') as file:
        data = json.load(file)
        users = []
        for user_json in data["users"]:
            name = user_json["user"]
            metrics = []
            for workload_json in user_json["workloads"]:
                metrics.append(get_workload_metrics(workload_json))
            users.append(User(name, metrics, base))
        return users
            




def compare_all(benches, scheduler="", partitioning="", config="" ):


    unfairness_rows = []
    for bench in benches:
        if scheduler in bench.scheduler and partitioning in bench.partitioning and config in bench.config:



            # track execution time
            start_time = min((workload.start_time for user in bench.users for workload in user.workload_metrics))
            end_time = max((workload.end_time for user in bench.users for workload in user.workload_metrics))
            total_time = (end_time - start_time)
            
            # calculate some indicators
            all_slowdowns = [x for user in bench.users for slowdowns in user.slowdowns for x in slowdowns]
            all_proportional_slowdowns = [x for user in bench.users for slowdowns in user.proportional_slowdowns for x in slowdowns]
            slowdown_mean = statistics.mean(all_slowdowns) 
            slowdown_worst_10_percent = get_worst_10_percent(all_slowdowns) 

            proportional_slowdown_worst_10_percent = get_worst_10_percent(all_proportional_slowdowns)
            unfairness_sum = 0

            # get nice colors
            cmap = plt.get_cmap("viridis", len(bench.users))
            user_colors = {user.name: cmap(i) for i, user in enumerate(bench.users)}
            plt.figure(figsize=(8, 5))
            for user in bench.users:
                base_color = user_colors[user.name]
                iteration = 1
                for workload_slowdown in user.slowdowns:
                    # convert to seconds
                    unfairness_sum += sum([(slow - slowdown_mean)**2 for slow in workload_slowdown])
                    # get similar color
                    tone = list(base_color)
                    tone[0] = np.clip(tone[0] + iteration * 0.2, 0, 1)
                    tone[1] = np.clip(tone[1] - iteration * 0.2, 0, 1)
                    tone = tuple(map(float, tone))
                    plt.plot(range(len(workload_slowdown)), workload_slowdown, color=tone ,marker='o', label=f"{user.name} ({iteration})" )
                    iteration+=1
                    
            unfairness = np.sqrt((unfairness_sum / len(all_slowdowns)))

            unfairness_rows.append([bench.config, 
                                    bench.partitioning, 
                                    bench.scheduler, 
                                    unfairness, 
                                    slowdown_mean, 
                                    slowdown_worst_10_percent,
                                    proportional_slowdown_worst_10_percent, 
                                    total_time,
                                    bench.peak_JVM_memory,
                                    bench.total_shuffle_read,
                                    bench.total_shuffle_write])

            plt.xlabel("Iteration")
            plt.ylabel("Slowdown (s)")
            plt.legend()
            plt.title(f"{bench.scheduler}: {bench.config}, {bench.partitioning}, unfairness={unfairness}")
            plt.show()
    df = pd.DataFrame(unfairness_rows, columns=["Config", 
                                                "Partitioning", 
                                                "Scheduler", 
                                                "Unfairness", 
                                                "Slowdown Mean",
                                                "Slowdown Worst 10%",
                                                "Proportional Worst 10%",
                                                "Total time",
                                                "Peak memory",
                                                "Shuffle reads",
                                                "Shuffle write"])
    df = df.sort_values(by=["Config", "Partitioning", "Unfairness"])
    df.to_excel(f"unfairness_{partitioning}.xlsx")
        




def timeline(benches):

    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(10, 8), sharex=True)

    for bench in benches:

        # track execution time

        start_time = min(jobgroup.start for user in bench.users for jobgroup in user.jobgroups)
        end_time = max(jobgroup.end for user in bench.users for jobgroup in user.jobgroups)
        total_time = end_time - start_time


       # get nice colors
        cmap = plt.get_cmap("viridis", len(bench.users))
        user_colors = {user.name: cmap(i) for i, user in enumerate(bench.users)}
        y_postion = 0
        executor_bins_map = {}
        for i in range(EXECUTOR_AMOUNT):
            executor_bins_map[i] = Bin(0,0)
        for user in bench.users:
            base_color = user_colors[user.name]
            jobgroup_bins = Bin(start_time, end_time)
            for jobgroup in user.jobgroups:

                # create bins to find available positions 
                for execution in jobgroup.executor_load:
                    start_offset = execution.start - start_time
                    end_offset = execution.end - start_time
                    bin_elem = Bin(start_offset, end_offset, id=execution.stage_id)
                    bin_elem.ex_type = execution.ex_type
                    bin_elem.color = base_color
                    executor_bins_map[execution.executor_id].add(bin_elem)


                # create bins to find available position
                stage_bins = Bin(jobgroup.start, jobgroup.end)
                for stage in jobgroup.stages:
                    start_offset = stage.start - start_time
                    end_offset = stage.end - start_time
                    stage_bins.add(Bin(start_offset, end_offset, id=stage.id))


                # add jobgroup bin, to set it to an available position in the graph
                start_offset = jobgroup.start - start_time
                end_offset = jobgroup.end - start_time
                jobgroup_bin = Bin(start_offset, end_offset, stage_bins.max)
                jobgroup_bins.add(jobgroup_bin)

                # calculate jobgroup offset and draw
                jobgroup_offset = y_postion + JOBGROUP_BIN_SIZE * jobgroup_bin.pos + JOBGROUP_BIN_DIST
                jobgroup_height = JOBGROUP_BIN_SIZE - 2 * JOBGROUP_BIN_DIST
                jobgroup_width = (jobgroup.end - jobgroup.start)

                axes[1].add_patch(patches.Rectangle(
                    (start_offset ,jobgroup_offset), jobgroup_width, jobgroup_height, color=base_color, alpha=0.4, label=f"Jobgroup {jobgroup.name}"
                ))

                for stage in stage_bins.subbins:

                    # to center the stages, add 1, so it ignores the ones on the edges of jobgroup
                    stage_offset = jobgroup_offset + (jobgroup_height / (stage_bins.max + 1)) * (stage.pos + 1)
                    axes[1].plot([stage.start, stage.end], [stage_offset,stage_offset], color='gray', linewidth=2)
                    axes[1].scatter(stage.start, stage_offset, color='green', s=10, zorder=2)  # Start marker
                    axes[1].scatter(stage.end, stage_offset, color='red', s=10, zorder=2)  # End marker
                    if SHOW_STAGE_IDS:
                        axes[1].annotate(stage.id, (stage.start,stage_offset), textcoords="offset points", xytext=(1,1), ha='left', fontsize=8, color="black")

            # move the y position by the amount of overlapping jobgroup bins
            y_postion+= JOBGROUP_BIN_SIZE * jobgroup_bins.max

        for executor_id in executor_bins_map:
            executor = executor_bins_map[executor_id]
            for execution in executor.subbins:
                exec_width = execution.end - execution.start
                exec_offset = CORES_PER_EXEC * executor_id + execution.pos - 0.5
                if EXECUTION_DATA == "group":
                    axes[0].add_patch(patches.Rectangle(
                        (execution.start, exec_offset), exec_width, 1.0, color=execution.color, alpha=0.5
                    ))
                elif EXECUTION_DATA == "type":
                    axes[0].add_patch(patches.Rectangle(
                        (execution.start, exec_offset), exec_width, 0.6, color="green" if execution.ex_type == "EXEC" else "yellow", alpha=0.5
                    ))

                if SHOW_STAGE_IDS:
                    axes[0].annotate(execution.id, (execution.start, exec_offset), textcoords="offset points", xytext=(1,1), ha='left', fontsize=8, color="black")
            # Draw a line between executors
            axes[0].axhline(y=CORES_PER_EXEC * (1 +executor_id) - 0.5, color='r', linestyle='--', linewidth=1)


        # setup executor plot
        axes[0].set_title(f"{bench.scheduler}: {bench.config}, {bench.partitioning}, runtime={total_time}")
        axes[0].set_ylabel("Core")
        axes[0].set_ylim(CORES_PER_EXEC * EXECUTOR_AMOUNT + 1, -1)


        # setup jobgroup plot
        axes[1].set_ylim(0, y_postion)
        axes[1].set_xlim(0, total_time)

        axes[1].set_xlabel('Time')
        axes[1].set_ylabel('Events')

        axes[1].grid(True, which='both', axis='x', linestyle='--', color='gray', alpha=0.5)
        plt.tight_layout()
        plt.show()

        # plt.savefig(filename + "user_job_timeline.png")

















    
AVAILABLE_FUNCTIONS=["unfairness", "timeline"]
if __name__ == "__main__":
    parser = argparse.ArgumentParser("Show certain benchmark results")
    parser.add_argument("function", help="Benchmark results to show", choices=AVAILABLE_FUNCTIONS)
    
    args = parser.parse_args()
    
    base_metrics = []
    for filename in os.listdir(BASE_BENCH_PATH):
        file_path = os.path.join(BASE_BENCH_PATH, filename)
        if os.path.isfile(file_path) and filename.endswith('.json'):
            base_metrics.append(get_bench_base(file_path))

    

    benches = []
    for filename in os.listdir(BENCH_PATH):
        file_path = os.path.join(BENCH_PATH, filename)
        if os.path.isfile(file_path) and filename.endswith('.json'):
            scheduler, partition, config = get_human_name(filename)
            try:
                users = get_bench_users(file_path, base_metrics)
                benches.append(Benchmark(scheduler, partition, config, users))
            except Exception as e:
                print(f"failed to parse benchmark {scheduler} {partition} {config}")
                    




    if args.function == "unfairness":
        compare_all(benches)
    elif args.function == "timeline":
        timeline(benches)



