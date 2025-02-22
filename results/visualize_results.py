import pandas as pd
import argparse
import requests
import json
import os
import statistics
import numpy as np
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

RUN_PATH="./data/performance_test_5/target"
BENCH_PATH=f"{RUN_PATH}/bench_outputs"

# history server address
APPS_URL="http://localhost:18080/api/v1/applications"




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


SCHEDULERS = [
    "CUSTOM_FAIR",
    "CUSTOM_RANDOM",
    "CUSTOM_SHORT",
    "DEFAULT_FIFO",
    "DEFAULT_FAIR",
    "AQE_CUSTOM_FAIR",
    "AQE_CUSTOM_RANDOM",
    "AQE_CUSTOM_SHORT",
    "AQE_DEFAULT_FIFO",
    "AQE_DEFAULT_FAIR",

]

CONFIGS = [
    "2_large_2_small_users",
    "4_large_users",
    "2_power_2_small_users",
    "4_super_small_users"
]

PARTITIONS = [
    "default",
    "coalesce",
    "repartition"
]


# names usually are bench_NAME_CONFIG_PARTITION
def get_human_name(filename):
    name = ""
    partition = ""
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

    # move index forawrd
    index += len(config) + len("_")

    for part in PARTITIONS:
        if filename[index:].startswith(part):
            partition = part
            break


    return name, partition, config

def get_worst_10_percent(arr):
    arr = np.array(arr)
    n = max(1, int(len(arr) * 0.1))
    top_10_percent = np.partition(arr, -n)[-n:]
    return np.mean(top_10_percent)


def find_closest_to_0(nums):
    candidate = 0 
    while candidate in nums:
        candidate+=1 
    return candidate 

def calculate_unfairness(slowdowns, mean):
    unfairness_sum = sum([(slow - mean)**2 for slow in slowdowns])


    return unfairness_sum / len(slowdowns)

def round_sig(x, sig=4):
    if x == 0:
        return 0  # Avoid log(0) error
    return round(x, sig - int(math.floor(math.log10(abs(x)))) - 1)


class Benchmark:
    def __init__(self, scheduler, partitioning, config, users_template):
        self.scheduler = scheduler
        self.partitioning = partitioning
        self.config = config
        self.app_name = f"bench_{scheduler}_{config}_{partitioning}"
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
        for app_id in app_ids:
            run = Run(self.scheduler, self.partitioning, self.config, np.copy(self.users_template))
            run.get_event_data(app_id)
            self.runs.append(run)


class Run:
    def __init__(self, scheduler, partitioning, config, users):
        self.scheduler = scheduler
        self.partitioning = partitioning
        self.config = config
        self.app_name = f"{scheduler}_{config}_{partitioning}"
        self.users = users

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
        self.jobs = jobs
        self.expected_runtime = expected_runtime_s
        


    def get_event_data(self, app_id):

        self.stages = []
        self.task_ids = []
        self.executor_load = []
        self.task_scheduler_delays = []
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

    benches = get_benchmarks(args.scheduler, args.config, args.part)

    
    unfairness_rows = []
    for bench in benches:
        worst_user_slowdown = 0
        for iteration, run in enumerate(bench.runs):
            print(f"Getting row elements for {run.app_name}, iteration: {iteration}")

            # get start times
            start_time = min(jobgroup.start for user in run.users for jobgroup in user.jobgroups)
            end_time = max(jobgroup.end for user in run.users for jobgroup in user.jobgroups)

            # general metrics
            total_time = (end_time - start_time)
            cpu_utilization = run.get_cpu_time() / (total_time * CORES_PER_EXEC * EXECUTOR_AMOUNT)
            write_ratio = run.get_write_ratio()
            avg_completion_time = sum([jobgroup.total_time for user in run.users for jobgroup in user.jobgroups]) / len([jobgroup.total_time for user in run.users for jobgroup in user.jobgroups])
            total_task_scheduler_delay = sum(delay for user in run.users for jobgroup in user.jobgroups for delay in jobgroup.task_scheduler_delays)

            # slowdown metrics
            all_slowdowns = [jobgroup.slowdown for user in run.users for jobgroup in user.jobgroups]
            all_proportional_slowdowns = [jobgroup.proportional_slowdown for user in run.users for jobgroup in user.jobgroups]

            slowdown_mean = statistics.mean(all_slowdowns) 
            slowdown_worst_10_percent = get_worst_10_percent(all_slowdowns) 
            proportional_slowdown_mean = statistics.mean(all_proportional_slowdowns)
            proportional_slowdown_worst_10_percent = get_worst_10_percent(all_proportional_slowdowns)

            # unfairness
            unfairness = calculate_unfairness(all_slowdowns, slowdown_mean)
            proportional_unfairness = calculate_unfairness(all_proportional_slowdowns, proportional_slowdown_mean)
            worst_user_unfairness = 0


            for user in run.users:
                # calculate user metrics
                user_proportional_slowdowns = [jobgroup.slowdown for jobgroup in user.jobgroups]
                worst_user_unfairness = max(worst_user_unfairness, calculate_unfairness(user_proportional_slowdowns, proportional_slowdown_mean))
                

            unfairness_rows.append([run.config, 
                                    run.partitioning, 
                                    run.scheduler, 
                                    iteration,
                                    unfairness, 
                                    proportional_unfairness,
                                    worst_user_unfairness,
                                    slowdown_mean, 
                                    slowdown_worst_10_percent,
                                    proportional_slowdown_worst_10_percent, 
                                    total_time,
                                    avg_completion_time,
                                    cpu_utilization,
                                    write_ratio,
                                    total_task_scheduler_delay,
                                    run.peak_JVM_memory,
                                    run.total_shuffle_read,
                                    run.total_shuffle_write])

    df = pd.DataFrame(unfairness_rows, columns=["Config", 
                                                "Partitioning", 
                                                "Scheduler", 
                                                "Iteration",
                                                "Unfairness", 
                                                "Proportional unfairness", 
                                                "Worst user unfairness",
                                                "Slowdown Mean",
                                                "Slowdown Worst 10%",
                                                "Proportional Worst 10%",
                                                "Total time",
                                                "Average complete time",
                                                "CPU utilization",
                                                "Proportion spent writing",
                                                "Total task scheduler delay",
                                                "Peak memory",
                                                "Shuffle reads",
                                                "Shuffle write"])
    df = df.sort_values(by=["Config", "Partitioning", "Unfairness"])
    df.to_excel(f"unfairness.xlsx")






def unfairness(args):

    benches = get_benchmarks(args.scheduler, args.config, args.part)

    if args.change_type == "user":
        for bench in benches:
            for iteration, run in enumerate(bench.runs):

                all_slowdowns = [jobgroup.slowdown for user in run.users for jobgroup in user.jobgroups]
                all_proportional_slowdowns = [jobgroup.proportional_slowdown for user in run.users for jobgroup in user.jobgroups]

                slowdown_mean = statistics.mean(all_slowdowns) 
                proportional_slowdown_mean = statistics.mean(all_proportional_slowdowns)

                # plot data
                cmap = plt.get_cmap("viridis", len(run.users))
                user_colors = {user.name: cmap(i) for i, user in enumerate(run.users)}
                color_array = [user_colors[user.name] for user in run.users]
                fig, ax = plt.subplots()
                plot_slowdowns = []
                plot_labels = []
                for user in run.users:
                    user_proportional_slowdowns = [jobgroup.slowdown for jobgroup in user.jobgroups]
                    user_unfairness = calculate_unfairness(user_proportional_slowdowns, proportional_slowdown_mean)
                    plot_slowdowns.append(user_proportional_slowdowns)
                    plot_labels.append(f"{user.name}, {round_sig(user_unfairness, 4)}")


                box = ax.boxplot(plot_slowdowns, tick_labels=plot_labels, patch_artist=True)

                for patch, color in zip(box['boxes'], color_array):
                    patch.set_facecolor(color)


                ax.set_title(f"User unfairness in:{run.app_name}")
                ax.set_xlabel("Users")
                ax.set_ylabel("Slowdown")

                if args.show_plot:
                    plt.show()
                plt.close(fig)
                

    else:
        
        
        plot_slowdowns = {}
        plot_labels = {}
        for bench in benches:
            for iteration, run in enumerate(bench.runs):

                slowdowns = []
                if args.change_type == "absolute":
                    slowdowns = [jobgroup.slowdown for user in run.users for jobgroup in user.jobgroups]
                elif args.change_type == "proportional":
                    slowdowns = [jobgroup.proportional_slowdown for user in run.users for jobgroup in user.jobgroups]
                    
                if run.config not in plot_slowdowns:
                    plot_slowdowns[run.config] = []
                    plot_labels[run.config] = []
                plot_slowdowns[run.config].append(slowdowns)
                plot_labels[run.config].append(run.app_name)


        for key in plot_slowdowns:
            fig, ax = plt.subplots()
            slowdowns = plot_slowdowns[key]
            labels = plot_labels[key]

            ax.boxplot(slowdowns)

            ax.set_xticks(range(1, len(labels) + 1))
            ax.set_xticklabels(labels, rotation=90)
            ax.set_xlabel("Configurations")


            ax.set_title("Slowdown per configuration")
            ax.set_ylabel("Slowdown (s)")
            plt.tight_layout()
            if args.show_plot:
                plt.show()
            plt.close(fig)
                



def timeline(args):

    benches = get_benchmarks(args.scheduler, args.config, args.part)

    
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

                    jobgroup_expected_endtime = jobgroup_start_offset + jobgroup.end - jobgroup.start
                    axes[1].plot([jobgroup_expected_endtime, jobgroup_expected_endtime], [jobgroup_offset, jobgroup_offset + jobgroup_height],alpha=0.5, color='red', linestyle="--", linewidth=1)

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
            axes[0].set_title(f"{run.scheduler} {iteration}: {run.config}, {run.partitioning}, utilization={run.get_cpu_time() / (total_time * CORES_PER_EXEC * EXECUTOR_AMOUNT)} runtime={total_time}")
            axes[0].set_ylabel("Core")
            axes[0].set_ylim(CORES_PER_EXEC * EXECUTOR_AMOUNT + 1, -1)


            # setup jobgroup plot
            axes[1].set_ylim(0, y_postion)
            axes[1].set_xlim(0, total_time)

            axes[1].set_xlabel('Time')
            axes[1].set_ylabel('Events')

            axes[1].grid(True, which='both', axis='x', linestyle='--', color='gray', alpha=0.5)
            fig.tight_layout()
            if args.show_plot:
                plt.show()

            filename = f"{run.scheduler}_{run.config}_{run.partitioning}" 
            fig.savefig(filename + "_user_job_timeline.png")



    
def get_bench_base(bench_path):
    with open(bench_path, 'r') as file:
        data = json.load(file)
        user = data["users"][0]
        workload = user["workloads"][0]
        name = workload["workloadName"]

        results = workload["results"]
        total_times_s = []
        for i in results:
            start_up = results[i]["setup_time"] 
            partition = results[i]["partitioning_time"] 
            exec = results[i]["execution_time"]
            total_times_s.append((start_up + partition + exec) * MS_TO_S)
        

        runtime = statistics.mean(total_times_s)

        
        return name, runtime
            


def get_bench_users(bench_path, base): 
    with open(bench_path, 'r') as file:
        data = json.load(file)
        users = []
        for user_json in data["users"]:
            name = user_json["user"]
            users.append(User(name, base))
        return users
            

def get_benchmarks(isolate_scheduler="", isolate_config="", isolate_partitioning=""):
 

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
            scheduler, partition, config = get_human_name(filename)
            tup = (scheduler, partition, config)
            if isolate_scheduler in scheduler and isolate_config in config and isolate_partitioning in partition:
                if tup not in unique_bench:
                    users = get_bench_users(file_path, base_runtimes)
                    benches.append(Benchmark(scheduler, partition, config, users))
                    unique_bench.append(tup)
                    
    return benches






if __name__ == "__main__":
    parser = argparse.ArgumentParser("Show certain benchmark results")
    parser.add_argument("--scheduler", help="Scheduler to isolate", action="store", default="")
    parser.add_argument("--config", help="Config to isolate", action="store", default="")
    parser.add_argument("--part", help="Partitioning to isolate", action="store", default="")
    parser.add_argument("--show_plot", help="Shows the plot using matplotlib GUI", action="store_true", default=False)

    subparsers = parser.add_subparsers(title="Commands")

    create_table_parser = subparsers.add_parser("create_table", help="Create an excel table that summerizes all results")
    create_table_parser.set_defaults(func=create_table)

    timeline_parser = subparsers.add_parser("timeline", help="Create event timeline images")
    timeline_parser.add_argument("--show_stage_id", help="Show stage id in timeline", action="store_true", default=False)
    timeline_parser.add_argument("--show_task_stage_id", help="Show stage id on tasks", action="store_true", default=False)
    timeline_parser.add_argument("--change_type", help="Show tasks in certain type. Values: type, group", action="store", default="group")
    timeline_parser.set_defaults(func=timeline)


    unfairness_parser = subparsers.add_parser("unfairness", help="Create unfairness boxplots")
    unfairness_parser.add_argument("--change_type", help="Show different type of unfairness metrics. Values: user, proportional, absolute", default="user")
    unfairness_parser.set_defaults(func=unfairness)

    
    args = parser.parse_args()
    args.func(args)
   

