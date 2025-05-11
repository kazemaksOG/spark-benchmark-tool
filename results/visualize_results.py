from os.path import isfile
import pandas as pd
import argparse
import requests
import json
import os
import numpy as np
import pickle
import math
from datetime import datetime
from copy import copy


import random
random.seed(42)

import matplotlib.pyplot as plt
import matplotlib.patches as patches


# result parsing settings

EXECUTOR_AMOUNT = 8
CORES_PER_EXEC = 4

RUN_PATH="./data/performance_test_26/target"
BENCH_PATH=f"{RUN_PATH}/bench_outputs"

# history server address
APPS_URL="http://localhost:18080/api/v1/applications"


SCHEDULERS = [
    "CUSTOM_FAIR",
    "CUSTOM_RANDOM",
    # "CUSTOM_SHORT",
    "DEFAULT_FIFO",
    "DEFAULT_FAIR_PARTITIONER",
    "DEFAULT_FAIR",
    # "AQE_CUSTOM_FAIR",
    # "AQE_CUSTOM_RANDOM",
    # "AQE_CUSTOM_SHORT",
    # "AQE_DEFAULT_FIFO",
    # "AQE_DEFAULT_FAIR",
    "CUSTOM_CLUSTERFAIR",
    "CUSTOM_USERCLUSTERFAIR_PARTITIONER", # has to be before regular because of string comparison
    "CUSTOM_USERCLUSTERFAIR",

]

FORMAL_NAME = {
    "DEFAULT_FAIR": "Fair",
    "DEFAULT_FAIR_PARTITIONER": "Fair-P",
    "CUSTOM_CLUSTERFAIR": "U-WFQ",
    "CUSTOM_USERCLUSTERFAIR_PARTITIONER": "U-WFQ-P",
}

CONFIGS = [
    "2_large_2_small_users",
    "4_large_users",
    "2_power_2_small_users",
    "4_super_small_users",
    "test_workload",
]


# _ used for delimiter
JOB_TYPES = [
"loop20_",
"loop100_",
"loop1000_",
]

# Numerical constants
GIGA = 1000000000
MS_TO_S = 1 / 1000 
NS_TO_S = 1 / 1000000000

S_TO_MS = 1000
S_TO_NS = 1000000000

# drawing constants
JOBGROUP_BIN_SIZE=5
JOBGROUP_BIN_DIST=0.3
STAGE_DIST=0.1



##################### HELPER FUNCTIONS #######################################

# names usually are bench_NAME_CONFIG_EXTRA
def get_human_name(filename):
    name = ""
    config = ""

    index = len("bench_")

    for sch in SCHEDULERS:
        if filename[index:].startswith(sch):
            name = sch 
            break

    # move index forawrd
    index += len(name) + len("_")

    for conf in CONFIGS:
        if filename[index:].startswith(conf):
            config = conf
            break

    return name, config

def get_job_type(name):
    for job_type in JOB_TYPES:
        if job_type in name:
            return job_type
    return "unclassified job"

def get_average(arr):
    return np.mean(arr)


def get_worst_10_percent(arr):
    arr = np.array(arr)
    n = max(1, int(len(arr) * 0.1))
    top_10_percent = np.partition(arr, -n)[-n:]
    return np.mean(top_10_percent)

def get_worst_1_percent(arr):
    arr = np.array(arr)
    n = max(1, int(len(arr) * 0.01))
    top_1_percent = np.partition(arr, -n)[-n:]
    return np.mean(top_1_percent)

def find_closest_to_0(nums):
    candidate = 0 
    while candidate in nums:
        candidate+=1 
    return candidate 


def round_sig(x, sig=4):
    if x == 0:
        return 0  # Avoid log(0) error
    return round(x, sig - int(math.floor(math.log10(abs(x)))) - 1)


########################## MAIN CLASSES ############################################
class Benchmark:
    def __init__(self, scheduler, config, users_template):
        self.scheduler = scheduler
        self.config = config
        self.app_name = f"bench_{scheduler}_{config}"
        self.runs = []
        self.users_template = users_template
        self.get_event_data()



    def get_event_data(self):

        response = requests.get(APPS_URL)

        if response.status_code != 200:  # Check if the request was successful
            raise Exception(f"Error requesting eventlog data for application: {response.status_code}")
        # find the jobs for data
        data = response.json()  # Convert response to JSON

        app_ids = [entry["id"] for entry in data if entry["name"].startswith(self.app_name)]
        print(f"Found app ids for {self.app_name}:")
        print(app_ids)
        for i, app_id in enumerate(app_ids):
            run = Run(self.scheduler, self.config, np.copy(self.users_template), i)
            run.get_event_data(app_id)
            self.runs.append(run)


class Run:
    def __init__(self, scheduler, config, users, iteration):
        self.scheduler = scheduler
        self.config = config
        self.app_name = f"{scheduler}_{config}"
        self.users = users
        self.iteration = iteration

    def get_cpu_time(self):
        return sum([execution.total_time for user in self.users for jobgroup in user.jobgroups for execution in jobgroup.executor_load])
    def get_write_ratio(self):
        write_time = sum([execution.total_time for user in self.users for jobgroup in user.jobgroups for execution in jobgroup.executor_load if execution.ex_type == "WRITE"])
        return write_time / self.get_cpu_time()

    def get_event_data(self, app_id):
        
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



    

class User:
    def __init__(self, name, base_runtimes):
        self.name = name
        self.base_runtimes = base_runtimes
        self.jobgroups = []



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
            
            for jobgroup_key in user_jobgroup_map:
                jobs = user_jobgroup_map[jobgroup_key]

                # Assume user only does one type of job
                base_runtime = 1
                if len(self.base_runtimes) != 0:
                    base_runtime = next((self.base_runtimes[base] for base in self.base_runtimes if base in jobgroup_key ))
                jobgroup = JobGroup(jobgroup_key, jobs, base_runtime)
                # get all jobgroup event data and then append
                jobgroup.get_event_data(app_id)
                self.jobgroups.append(jobgroup)
                

        else:
            raise Exception(f"Error requesting eventlog data for jobs: {response.status_code}")




class JobGroup:
    def __init__(self, name, jobs, expected_runtime_s):
        self.name = name
        self.job_type = get_job_type(name)
        self.jobs = jobs
        self.expected_runtime = expected_runtime_s
        


        self.stages = []
        self.task_ids = []
        self.executor_load = []
        self.task_scheduler_delays = []
        self.start = None
        self.end = None

        self.total_time = None
        self.slowdown = None
        self.proportional_slowdown = None

    def get_event_data(self, app_id):

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
                    print(f"Stage {stage_id} was either skipped or failed (or something else) for job {job_id}, response: {response.status_code}") 
                    continue
                    # raise Exception(f"Error requesting eventlog data for stage {stage_id} for job {job_id}: {response.status_code}")
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
                        task_id = task["taskId"]

                        # check if any duplicate
                        if task_id in self.task_ids:
                            print(task_id)
                            continue

                        task_start = task["launchTime"]
                        task_start_dt = datetime.strptime(task_start[:-3], "%Y-%m-%dT%H:%M:%S.%f")
                        task_start_s =task_start_dt.timestamp()
                        executor_id = int(task["executorId"])

                        task_metrics = task["taskMetrics"]
                        # get write and compute times. because other parts of execution are negligible and i had issues with allignment, execution time includes everything except writing
                        execution_time_s = task_metrics["executorRunTime"] / S_TO_MS
                        write_time_s = task_metrics["shuffleWriteMetrics"]["writeTime"] * NS_TO_S

                        execution_start_s = task_start_s
                        execution_end_s = task_start_s + execution_time_s - write_time_s

                        write_start_s = execution_end_s 
                        write_end_s = write_start_s + write_time_s 

                        self.executor_load.append(Execution(executor_id, "EXEC", execution_start_s, execution_end_s, stage_id))
                        self.executor_load.append(Execution(executor_id, "WRITE", write_start_s, write_end_s, stage_id))

                        # add the scheduler delay
                        task_scheduler_delay_s = task["schedulerDelay"] / S_TO_MS
                        self.task_scheduler_delays.append(task_scheduler_delay_s)

                        self.task_ids.append(task_id)

        # calculate jobgroup metrics
        self.total_time = self.end - self.start
        self.slowdown = self.total_time - self.expected_runtime
        self.proportional_slowdown = self.total_time / self.expected_runtime




 
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
        self.total_time = end_s - start_s
        self.stage_id = stage_id



class Bin:
    def __init__(self, start, end, max=1, id=0, e=0.005):
        self.e = e
        self.start = start 
        self.end = end 
        self.max = max
        self.id = id
        self.pos = -1
        self.subbins = []

    def add(self, bin_elem):
        self.subbins.append(bin_elem)


    def pack_subbins(self):
        self.subbins = [bin for bin in self.subbins if bin.end - bin.start > self.e]
        ends = copy(self.subbins)
        n = len(self.subbins)
        self.subbins.sort(key=lambda x: x.start)
        ends.sort(key=lambda x: x.end)

        i = 0
        j = 0

        taken_pos = set()
        while i < n :

            if self.subbins[i].start >= ends[j].end and ends[j].pos in taken_pos:
                taken_pos.remove(ends[j].pos)
                j += 1
            else:

                pos = find_closest_to_0(taken_pos)
                self.subbins[i].pos = pos
                taken_pos.add(pos)
                self.max = self.max if self.max > len(taken_pos) else len(taken_pos)
                i += 1



def create_table(args):

    benches = get_benchmarks(args.scheduler, args.config)

    
    for bench in benches:

        run_rows = []
        baseline_benches = get_benchmarks(args.compare_to, bench.config)
        for iteration, run in enumerate(bench.runs):
            print(f"Getting row elements for {run.app_name}, iteration: {iteration}")

            run_row = []

            # get start times
            start_time = min(jobgroup.start for user in run.users for jobgroup in user.jobgroups)
            end_time = max(jobgroup.end for user in run.users for jobgroup in user.jobgroups)

            # general metrics
            total_time = (end_time - start_time)
            cpu_time = run.get_cpu_time() 
            write_ratio = run.get_write_ratio()
            total_task_scheduler_delay = sum(delay for user in run.users for jobgroup in user.jobgroups for delay in jobgroup.task_scheduler_delays)

            run_row.extend([
                ("Config", run.config),
                ("Scheduler", run.config),
                ("Iteration", run.iteration),
                ("total time", total_time), 
                ("cpu time", cpu_time),
                ("write to cpu ratio", write_ratio),
                ("task scheduler delay", total_task_scheduler_delay),
                ("peak JVM", run.peak_JVM_memory),
                ("total shuffle read", run.total_shuffle_read),
                ("total shuffle write", run.total_shuffle_write),
            ])



            # slowdown metrics
            all_slowdowns = [jobgroup.slowdown for user in run.users for jobgroup in user.jobgroups]
            all_proportional_slowdowns = [jobgroup.proportional_slowdown for user in run.users for jobgroup in user.jobgroups]


            avg_rt = get_average([jobgroup.total_time for user in run.users for jobgroup in user.jobgroups])
            slowdown_avg = get_average(all_slowdowns) 
            slowdown_avg_10 = get_worst_10_percent(all_slowdowns) 
            proportional_slowdown_avg = get_average(all_proportional_slowdowns)
            proportional_slowdown_avg_10 = get_worst_10_percent(all_proportional_slowdowns)
            

            run_row.extend([
                ("average response time", avg_rt),

                ("average absolute slowdown", slowdown_avg),
                ("average absolute slowdown (worst10%)", slowdown_avg_10),

                ("average proportional slowdown", proportional_slowdown_avg),
                ("average proportional slowdown (worst10%)", proportional_slowdown_avg_10),
            ])

            # per job type metrics
            for job_type in JOB_TYPES:
                jobs = [jobgroup.total_time for user in run.users for jobgroup in user.jobgroups if jobgroup.job_type == job_type]
                jobs_slowdown =  [jobgroup.slowdown for user in run.users for jobgroup in user.jobgroups if jobgroup.job_type == job_type]
                jobs_prop_slowdown =  [jobgroup.proportional_slowdown for user in run.users for jobgroup in user.jobgroups if jobgroup.job_type == job_type]


                jobs_rt_avg = get_average(jobs)
                jobs_rt_avg_10 = get_worst_10_percent(jobs)
                jobs_rt_avg_1 = get_worst_1_percent(jobs)


                jobs_slowdown_avg = get_average(jobs_slowdown)
                jobs_slowdown_avg_10 = get_worst_10_percent(jobs_slowdown)
                jobs_slowdown_avg_1 = get_worst_1_percent(jobs_slowdown)


                jobs_prop_slowdown_avg = get_average(jobs_prop_slowdown)
                jobs_prop_slowdown_avg_10 = get_worst_10_percent(jobs_prop_slowdown)
                jobs_prop_slowdown_avg_1 = get_worst_1_percent(jobs_prop_slowdown)

                run_row.extend([
                    (f"{job_type} average response time", jobs_rt_avg),
                    (f"{job_type} average response time (worst 10%)", jobs_rt_avg_10),
                    (f"{job_type} average response time (worst 1%)", jobs_rt_avg_1),

                    (f"{job_type} average absolute slowdown", jobs_slowdown_avg),
                    (f"{job_type} average absolute slowdown (worst 10%)", jobs_slowdown_avg_10),
                    (f"{job_type} average absolute slowdown (worst 1%)", jobs_slowdown_avg_1),

                    (f"{job_type} average proportional slowdown", jobs_prop_slowdown_avg),
                    (f"{job_type} average proportional slowdown (worst 10%)", jobs_prop_slowdown_avg_10),
                    (f"{job_type} average proportional slowdown (worst 1%)", jobs_prop_slowdown_avg_1),
                ])



            for user in run.users:
                # calculate user metrics
                user_runtime = [jobgroup.total_time for jobgroup in user.jobgroups]
                user_slowdown =  [jobgroup.slowdown for jobgroup in user.jobgroups ]
                user_prop_slowdown =  [jobgroup.proportional_slowdown for jobgroup in user.jobgroups]


                user_rt_avg = get_average(user_runtime)
                user_rt_avg_10 = get_worst_10_percent(user_runtime)
                user_rt_avg_1 = get_worst_1_percent(user_runtime)


                user_slowdown_avg = get_average(user_slowdown)
                user_slowdown_avg_10 = get_worst_10_percent(user_slowdown)
                user_slowdown_avg_1 = get_worst_1_percent(user_slowdown)


                user_prop_slowdown_avg = get_average(user_prop_slowdown)
                user_prop_slowdown_avg_10 = get_worst_10_percent(user_prop_slowdown)
                user_prop_slowdown_avg_1 = get_worst_1_percent(user_prop_slowdown)

                run_row.extend([
                    (f"{user.name} average response time", user_rt_avg),
                    (f"{user.name} average response time (worst 10%)", user_rt_avg_10),
                    (f"{user.name} average response time (worst 1%)", user_rt_avg_1),

                    (f"{user.name} average absolute slowdown", user_slowdown_avg),
                    (f"{user.name} average absolute slowdown (worst 10%)", user_slowdown_avg_10),
                    (f"{user.name} average absolute slowdown (worst 1%)", user_slowdown_avg_1),

                    (f"{user.name} average proportional slowdown", user_prop_slowdown_avg),
                    (f"{user.name} average proportional slowdown (worst 10%)", user_prop_slowdown_avg_10),
                    (f"{user.name} average proportional slowdown (worst 1%)", user_prop_slowdown_avg_1),
                ])

            # compare to fair scheduler
            deadline_miss = []
            deadline_gain = []
            if run.scheduler != args.compare_to:
                baseline_run = (baseline_benches[0]).runs[0]
                for user in run.users:
                    for job_type in JOB_TYPES:
                        job_deadline_miss = []
                        job_deadline_gain = []
                        for jobgroup in user.jobgroups:
                            if jobgroup.job_type != job_type:
                                continue

                            baseline_jobgroup = [base_jobgroup for user in baseline_run.users for base_jobgroup in user.jobgroups if base_jobgroup.name == jobgroup.name]
                            if len(baseline_jobgroup) == 0:
                                print("skipping: " + jobgroup.name + " for: " + baseline_run.scheduler)
                                continue

                            baseline_jobgroup = baseline_jobgroup[0]
                            if jobgroup.end > baseline_jobgroup.end:
                                deadline_miss.append(jobgroup.end - baseline_jobgroup.end)
                                job_deadline_miss.append(jobgroup.end - baseline_jobgroup.end)
                            else:
                                deadline_gain.append(baseline_jobgroup.end - jobgroup.end)
                                job_deadline_gain.append(baseline_jobgroup.end - jobgroup.end)

                        job_sum_deadline_miss = sum(job_deadline_miss)
                        job_avg_deadline_miss = get_average(job_deadline_miss) 
                        job_avg_10_deadline_miss = get_worst_10_percent(job_deadline_miss)
                        job_avg_1_deadline_miss = get_worst_1_percent(job_deadline_miss)


                        job_sum_deadline_gain = sum(job_deadline_gain)
                        job_avg_deadline_gain = get_average(job_deadline_gain) 
                        job_avg_10_deadline_gain = get_worst_10_percent(job_deadline_gain)
                        job_avg_1_deadline_gain = get_worst_1_percent(job_deadline_gain)


                        run_row.extend([
                            (f"{job_type} Total missed deadline time", job_sum_deadline_miss),
                            (f"{job_type} Average missed deadline time", job_avg_deadline_miss),
                            (f"{job_type} Worst 10% missed deadline time", job_avg_10_deadline_miss),
                            (f"{job_type} Worst 1% missed deadline time", job_avg_1_deadline_miss),

                            (f"{job_type} Total gained deadline time", job_sum_deadline_gain),
                            (f"{job_type} Average gained deadline time", job_avg_deadline_gain),
                            (f"{job_type} Worst 10% gained deadline time", job_avg_10_deadline_gain),
                            (f"{job_type} Worst 1% gained deadline time", job_avg_1_deadline_gain),
                        ])

                        
            sum_deadline_miss = sum(deadline_miss)
            avg_deadline_miss = get_average(deadline_miss) 
            avg_10_deadline_miss = get_worst_10_percent(deadline_miss)
            avg_1_deadline_miss = get_worst_1_percent(deadline_miss)


            sum_deadline_gain = sum(deadline_gain)
            avg_deadline_gain = get_average(deadline_gain) 
            avg_10_deadline_gain = get_worst_10_percent(deadline_gain)
            avg_1_deadline_gain = get_worst_1_percent(deadline_gain)

            run_row.extend([
                ("Total missed deadline time", sum_deadline_miss),
                ("Average missed deadline time", avg_deadline_miss),
                ("Worst 10% missed deadline time", avg_10_deadline_miss),
                ("Worst 1% missed deadline time", avg_1_deadline_miss),

                ("Total gained deadline time", sum_deadline_gain),
                ("Average gained deadline time", avg_deadline_gain),
                ("Worst 10% gained deadline time", avg_10_deadline_gain),
                ("Worst 1% gained deadline time", avg_1_deadline_gain),
            ])



            # append the row
            run_rows.append(run_row)

        print(bench.app_name)
        columns = [col[0] for col in run_rows[0]]
        values = [[val[1] for val in row] for row in run_rows]
        df = pd.DataFrame(values, columns=columns)

        df = df.sort_values(by=["Config", "Scheduler"])


        # make a dir if necessary
        output_folder = f"{bench.scheduler}_{bench.config}"
        os.makedirs(output_folder, exist_ok=True)

        df.to_csv(os.path.join(output_folder, f"run_data.csv"))





def plot_and_save_cdf(target, baseline, target_name, base_name, folder, output):
    # plot data
    fig, ax = plt.subplots()

    ax.ecdf(target, label=f"{target_name}", linestyle="-", color="royalblue")

    ax.ecdf(baseline, label=f"{base_name}", linestyle="--", color="black")


    ax.grid(True)
    ax.legend()
    # ax.set_title(f"Response time ECDF :{run.config}")
    ax.set_xlabel("Response time (s)")
    ax.set_ylabel("Fraction of jobs")
    ax.set_ylim(ymin=0)
    ax.set_xlim(xmin=0)

    if args.show_plot:
        plt.show()

    # make a dir if necessary
    os.makedirs(folder, exist_ok=True)

    filename = os.path.join(folder, f"{output}.png") 
    print(f"saving {filename}")
    fig.savefig(filename)
    plt.close(fig)


def cdf(args):

    benches = get_benchmarks(args.scheduler, args.config)

    if args.change_type == "total":
        for bench in benches:
            for run in bench.runs:

                # no need to cmpare baseline to itself
                if run.scheduler == args.compare_to:
                    continue

                all_rt = [jobgroup.total_time for user in run.users for jobgroup in user.jobgroups]
                # all_slowdowns = [jobgroup.slowdown for user in run.users for jobgroup in user.jobgroups]
                # all_proportional_slowdowns = [jobgroup.proportional_slowdown for user in run.users for jobgroup in user.jobgroups]


                # get data for baseline

                baseline_benches = get_benchmarks(args.compare_to, run.config)
                baseline_run = (baseline_benches[0]).runs[0]
                baseline_rt = [jobgroup.total_time for user in baseline_run.users for jobgroup in user.jobgroups]

                plot_and_save_cdf(all_rt, baseline_rt, run.scheduler, args.compare_to, f"{run.scheduler}_{run.config}", "overall_rt")


                

    elif args.change_type == "user":
        
        for bench in benches:
            for run in bench.runs:

                # no need to cmpare baseline to itself
                if run.scheduler == args.compare_to:
                    continue

                for user in run.users:
                    user_rt = [jobgroup.total_time for jobgroup in user.jobgroups]

                    # get data for baseline

                    baseline_benches = get_benchmarks(args.compare_to, run.config)
                    baseline_run = (baseline_benches[0]).runs[0]
                    baseline_user = [base_user for base_user in baseline_run.users if base_user.name == user.name ][0]
                    baseline_user_rt = [jobgroup.total_time for jobgroup in baseline_user.jobgroups]

                    plot_and_save_cdf(user_rt, baseline_user_rt, run.scheduler, args.compare_to, f"{run.scheduler}_{run.config}",f"{user.name}_response_time_cdf")

    elif args.change_type == "job":
        for bench in benches:
            for run in bench.runs:

                # no need to cmpare baseline to itself
                if run.scheduler == args.compare_to:
                    continue

                for job_type in JOB_TYPES:
                    job_rt = [jobgroup.total_time for user in run.users for jobgroup in user.jobgroups if jobgroup.job_type in job_type]

                    # if no such job for this benchmark, continue
                    if len(job_rt) == 0:
                        continue

                    # get data for baseline

                    baseline_benches = get_benchmarks(args.compare_to, run.config)
                    baseline_run = (baseline_benches[0]).runs[0]
                    baseline_job_rt = [jobgroup.total_time for user in baseline_run.users for jobgroup in user.jobgroups if jobgroup.job_type == job_type]

                    plot_and_save_cdf(job_rt, baseline_job_rt, run.scheduler, args.compare_to, f"{run.scheduler}_{run.config}",f"{job_type}_response_time_cdf")

                





def timeline(args):

    benches = get_benchmarks(args.scheduler, args.config)

    
    for bench in benches:
        for iteration, run in enumerate(bench.runs):
            print(f"Making timeline for {run.app_name}")
            fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(10, 8), sharex=True)

            # track execution time
            if len([jobgroup.start for user in run.users for jobgroup in user.jobgroups]) == 0:
                print(f"No jobgroups found for {run.app_name}")
                continue
            start_time = min(jobgroup.start for user in run.users for jobgroup in user.jobgroups)
            end_time = max(jobgroup.end for user in run.users for jobgroup in user.jobgroups)
            total_time = end_time - start_time


           # get nice colors
            cmap = plt.get_cmap("viridis", len(run.users))
            user_colors = {user.name: cmap(i) for i, user in enumerate(run.users)}
            
            # for controlling allignment
            y_postion = 0
            executor_bins_map = {}
            for i in range(EXECUTOR_AMOUNT):
                executor_bins_map[i] = Bin(0,0)
            for user in run.users:
                base_color = user_colors[user.name]
                jobgroup_bins = Bin(start_time, end_time)
                for jobgroup in user.jobgroups:
                    # create bins for executors
                    for execution in jobgroup.executor_load:
                        bin_elem = Bin(execution.start, execution.end, id=execution.stage_id)
                        bin_elem.ex_type = execution.ex_type
                        bin_elem.color = base_color
                        executor_bins_map[execution.executor_id].add(bin_elem)


                    # create bins for stages
                    jobgroup_bin = Bin(jobgroup.start, jobgroup.end)
                    for stage in jobgroup.stages:
                        jobgroup_bin.add(Bin(stage.start, stage.end, id=stage.id))

                    jobgroup_bin.pack_subbins()
                    jobgroup_bin.name = jobgroup.name

                    jobgroup_bin.expected_runtime = jobgroup.expected_runtime

                    # add jobgroup to its bin
                    jobgroup_bins.add(jobgroup_bin)
                jobgroup_bins.pack_subbins()


                # iterate over jobgroup bins and draw them
                for jobgroup in jobgroup_bins.subbins:

                    jobgroup_offset = y_postion + JOBGROUP_BIN_SIZE * jobgroup.pos + JOBGROUP_BIN_DIST
                    jobgroup_height = JOBGROUP_BIN_SIZE - 2 * JOBGROUP_BIN_DIST
                    jobgroup_width = (jobgroup.end - jobgroup.start)

                    jobgroup_start_offset = jobgroup.start - start_time
                    axes[1].add_patch(patches.Rectangle(
                        (jobgroup_start_offset, jobgroup_offset), jobgroup_width, jobgroup_height, color=base_color, alpha=0.4, label=f"Jobgroup {jobgroup.name}"
                    ))

                    jobgroup_endtime = jobgroup_start_offset + jobgroup.end - jobgroup.start
                    jobgroup_expected_endtime = jobgroup_start_offset + jobgroup.expected_runtime

                    # if job finished faster than expected, we give it a different color and ignore expected runtime
                    if jobgroup_endtime > jobgroup_expected_endtime:
                        axes[1].plot([jobgroup_endtime, jobgroup_endtime], [jobgroup_offset, jobgroup_offset + jobgroup_height],alpha=0.5, color='red', linestyle="-", linewidth=2)
                        axes[1].plot([jobgroup_expected_endtime, jobgroup_expected_endtime], [jobgroup_offset, jobgroup_offset + jobgroup_height],alpha=0.5, color='orange', linestyle="-", linewidth=2)
                    else:
                        axes[1].plot([jobgroup_endtime, jobgroup_endtime], [jobgroup_offset, jobgroup_offset + jobgroup_height],alpha=0.5, color='green', linestyle="-", linewidth=2)


                    if args.show_stage:
                        for stage in jobgroup.subbins:

                            # to center the stages, add 1, so it ignores the ones on the edges of jobgroup
                            stage_offset = jobgroup_offset + (jobgroup_height / (jobgroup.max + 1)) * (stage.pos + 1)
                            stage_start_offset = stage.start - start_time
                            stage_end_offset = stage.end - start_time
                            axes[1].plot([stage_start_offset, stage_end_offset], [stage_offset,stage_offset], color='gray', linewidth=2)
                            axes[1].scatter(stage_start_offset, stage_offset, color='green', s=10, zorder=2)  # Start marker
                            axes[1].scatter(stage_end_offset, stage_offset, color='red', s=10, zorder=2)  # End marker
                            if args.show_stage_id:
                                axes[1].annotate(stage.id, (stage_start_offset,stage_offset), textcoords="offset points", xytext=(1,1), ha='left', fontsize=8, color="black")

                # move the y position by the amount of overlapping jobgroup bins
                y_postion+= JOBGROUP_BIN_SIZE * jobgroup_bins.max




            # draw executors
            for executor_id in executor_bins_map:
                executor = executor_bins_map[executor_id]
                executor.pack_subbins()
                for execution in executor.subbins:
                    exec_width = execution.end - execution.start
                    exec_offset = CORES_PER_EXEC * executor_id + execution.pos - 0.5
                    exec_start_offset = execution.start - start_time
                    if args.change_type== "group":
                        axes[0].add_patch(patches.Rectangle(
                            (exec_start_offset, exec_offset), exec_width, 1.0, color=execution.color, alpha=0.5
                        ))
                    elif args.change_type == "type":
                        axes[0].add_patch(patches.Rectangle(
                            (exec_start_offset, exec_offset), exec_width, 0.6, color="green" if execution.ex_type == "EXEC" else "yellow", alpha=0.5
                        ))

                    if args.show_task_stage_id:
                        axes[0].annotate(execution.id, (exec_start_offset, exec_offset), textcoords="offset points", xytext=(1,1), ha='left', fontsize=8, color="black")
                # Draw a line between executors
                axes[0].axhline(y=CORES_PER_EXEC * (1 +executor_id) - 0.5, color='r', linestyle='--', linewidth=1)


            # setup executor plot
            # axes[0].set_title(f"{run.scheduler} {iteration}: {run.config}, utilization={run.get_cpu_time() / (total_time * CORES_PER_EXEC * EXECUTOR_AMOUNT)} runtime={total_time}")
            axes[0].set_ylabel("Core")
            axes[0].set_ylim(CORES_PER_EXEC * EXECUTOR_AMOUNT + 1, -1)


            # setup jobgroup plot
            axes[1].set_ylim(0, y_postion)
            axes[1].set_xlim(0, total_time)

            axes[1].set_xlabel('Time (s)')
            axes[1].set_ylabel('Jobs')
            axes[1].set_yticks([]) 

            axes[1].grid(True, which='both', axis='x', linestyle='--', color='gray', alpha=0.5)
            fig.tight_layout()
            if args.show_plot:
                plt.show()



            # make a dir if necessary
            output_folder = f"{run.scheduler}_{run.config}"
            os.makedirs(output_folder, exist_ok=True)

            filename = os.path.join(output_folder, f"user_job_timeline.png") 

            fig.savefig(filename)
            plt.close(fig)



    
# Get the average of base runtimes, extract the name from workfloadName for identifier
def get_bench_base(bench_path):
    with open(bench_path, 'r') as file:
        data = json.load(file)
        user = data["users"][0]
        workload = user["workloads"][0]
        # append "_" for delimiting, since some workloads share base name (loop100, loop1000)
        name = workload["workloadName"] + "_"

        results = workload["results"]
        total_times_s = []
        for i in results:
            start_up = results[i]["setup_time"] 
            partition = results[i]["partitioning_time"] 
            exec = results[i]["execution_time"]
            total_times_s.append((start_up + partition + exec) * MS_TO_S)
        

        runtime = get_average(total_times_s)

        
        return name, runtime
            


def get_bench_users(bench_path, base): 
    with open(bench_path, 'r') as file:
        data = json.load(file)
        users = []
        for user_json in data["users"]:
            name = user_json["user"]
            users.append(User(name, base))
        return users
            

def get_benchmarks(isolate_scheduler="", isolate_config=""):

    # see if a previous benchmark data exist
    data_dump_path = os.path.join(BENCH_PATH, "DATADUMP.data")
    if os.path.exists(data_dump_path) and os.path.isfile(data_dump_path):
        print("Loading data: " + data_dump_path)
        with open(data_dump_path, "rb") as file:
            benches = pickle.load(file)
            filtered = [bench for bench in benches if isolate_config in bench.config and (isolate_scheduler == "" or isolate_scheduler == bench.scheduler)]
            return filtered


    # create benchmarks
    base_runtimes = {}
    for filename in os.listdir(BENCH_PATH):
        file_path = os.path.join(BENCH_PATH, filename)
        if os.path.isfile(file_path) and filename.endswith('.json'):
            if "BASE" in filename:
                name, runtime = get_bench_base(file_path)
                base_runtimes[name] = runtime

    benches = []
    unique_bench = []
    for filename in os.listdir(BENCH_PATH):
        file_path = os.path.join(BENCH_PATH, filename)
        if os.path.isfile(file_path) and filename.endswith('.json'):
            scheduler, config = get_human_name(filename)
            if scheduler == "":
                continue
            tup = (scheduler, config)
            if isolate_scheduler in scheduler and isolate_config in config:
                if tup not in unique_bench:
                    users = get_bench_users(file_path, base_runtimes)
                    benches.append(Benchmark(scheduler, config, users))
                    unique_bench.append(tup)

    # save data for future runs
    with open(data_dump_path, "wb") as file:
        print("Saving data: " + data_dump_path)
        pickle.dump(benches, file)

                    
    return benches






if __name__ == "__main__":
    parser = argparse.ArgumentParser("Show certain benchmark results")
    parser.add_argument("--scheduler", help="Scheduler to isolate", action="store", default="")
    parser.add_argument("--config", help="Config to isolate", action="store", default="")
    parser.add_argument("--show_plot", help="Shows the plot using matplotlib GUI", action="store_true", default=False)

    subparsers = parser.add_subparsers(title="Commands")

    create_table_parser = subparsers.add_parser("create_table", help="Create an excel table that summerizes all results")
    create_table_parser.add_argument("--compare_to", help="Scheduler to be taken as base", default="DEFAULT_FAIR" )
    create_table_parser.set_defaults(func=create_table)

    timeline_parser = subparsers.add_parser("timeline", help="Create event timeline images")

    timeline_parser.add_argument("--show_stage", help="Show stage in timeline", action="store_true", default=False)
    timeline_parser.add_argument("--show_stage_id", help="Show stage id in timeline", action="store_true", default=False)
    timeline_parser.add_argument("--show_task_stage_id", help="Show stage id on tasks", action="store_true", default=False)
    timeline_parser.add_argument("--change_type", help="Show tasks in certain type. Values: type, group", action="store", default="group")
    timeline_parser.set_defaults(func=timeline)


    cdf_parser = subparsers.add_parser("cdf", help="Create ECDFs of response time")
    cdf_parser.add_argument("--change_type", help="Show different type of cdf metrics. Values: user, job, total", default="total")
    cdf_parser.add_argument("--compare_to", help="Scheduler to be taken as base", default="DEFAULT_FAIR" )
    cdf_parser.set_defaults(func=cdf)


    # unfairness_parser = subparsers.add_parser("unfairness", help="Create unfairness boxplots")
    # unfairness_parser.add_argument("--change_type", help="Show different type of unfairness metrics. Values: user, proportional, absolute", default="user")
    # unfairness_parser.set_defaults(func=unfairness)

    
    args = parser.parse_args()
    args.func(args)
   







