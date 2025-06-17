from utility import *
from globals import *


import pandas as pd
import os
import json
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.ticker as ticker


WORKLOAD_NAME = "loop_custom"
WORKLOAD_INPUT_PATH = "resources/tripdata-partitionBy-PULocationID.parquet"
WORKLOAD_INPUT_TYPE = "PARQUET"
WORKLOAD_CLASS = "jobs.implementations.udf.LoopCustom"
WORKLOAD_ITERATIONS = 1
WORKLOAD_RATE = 0
WORKLOAD_FREQUENCY = "PARA"

plt.rcParams.update({'font.size': 20})


class Job:
    def __init__(self, id, bench_start_time):
        self.id = id
        self.tasks = []
        self.bench_start_time = bench_start_time
        self.runtime = 0



    def add_task(self, row):
        start_time = (row["ts_submit_seconds"] - self.bench_start_time) * TIME_SCALE
        runtime = row["resource_run_time"] * MS_TO_S
        scaled_runtime = SCALING * runtime / CORES

        end_time = start_time + scaled_runtime
        self.runtime += scaled_runtime

        self.tasks.append((start_time, end_time))

    def get_times(self):
        start = min(t[0] for t in self.tasks)
        end = start + self.runtime

        return (start, end)
    def get_runtime(self):
        start, end = self.get_times()
        return end - start




class User:
    def __init__(self, name, bench_start_time):
        self.name = name
        self.workflow_id_to_job = {}
        self.bench_start_time = bench_start_time

    def add_task(self, row):
        workflow_id = row["workflow_id"]
        job = None
        if workflow_id in self.workflow_id_to_job.keys():
            job = self.workflow_id_to_job[workflow_id]
        else:
            job = Job(workflow_id, self.bench_start_time)
            self.workflow_id_to_job[workflow_id] = job

        job.add_task(row)

            


def parse_users(df, bench_start_time):
    user_id_to_user = {}

    user_count = 0
    for index, row in df.iterrows():
        user_id = row["user_id"]

        user = None
        if user_id in user_id_to_user.keys():
            user = user_id_to_user[user_id]
        else:
            user_count += 1
            user = User(f"user_{user_count}", bench_start_time)
            user_id_to_user[user_id] = user
        user.add_task(row)
    return user_id_to_user

def print_stats(user_list):


    print("total amount of resource time:")
    total_runtime = [job.get_runtime() for user in user_list.values() for job in user.workflow_id_to_job.values()]
    print(sum(total_runtime))
    print("resource time per second:")
    print(sum(total_runtime) / TIME_FRAME_S)


def plot_histogram(user_list):

    job_runtimes = []

    for user_id, user in user_list.items():
        for jobgroup in user.workflow_id_to_job.values():
            jobgroup_start, jobgroup_end = jobgroup.get_times()
            job_runtimes.append(jobgroup_end - jobgroup_start)
            
        
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(job_runtimes, bins=100, edgecolor='black', alpha=0.7)
    ax.axvline(x=5, color='red', linestyle='--', linewidth=1, label='Reference Line at x=0')

    ax.set_xlabel("Runtime (s)")
    ax.set_ylabel("Amount of jobs")
    ax.grid(True)
    plt.show()


    os.makedirs(OUTPUT_DIR, exist_ok=True)
    fig.savefig(os.path.join(OUTPUT_DIR, f"macro_benchmark_historgram.{FIG_FORMAT}"))


def plot_timeline(user_list):


    cmap = plt.get_cmap("viridis", len(user_list))
    user_colors = {user: cmap(i) for i, user in enumerate(user_list.keys())}

    fig, ax = plt.subplots()
    y_postion = 0
    for user_id, user in user_list.items():
        base_color = user_colors[user_id]
        jobgroup_bins = Bin(0, TIME_FRAME_S)
        for jobgroup in user.workflow_id_to_job.values():
            # create bins for stages

            jobgroup_start, jobgroup_end = jobgroup.get_times()
            jobgroup_bin = Bin(jobgroup_start, jobgroup_end)
            for start, end in jobgroup.tasks:
                jobgroup_bin.add(Bin(start, end))

            jobgroup_bin.pack_subbins()
            # add jobgroup to its bin
            jobgroup_bins.add(jobgroup_bin)
        jobgroup_bins.pack_subbins()


        # iterate over jobgroup bins and draw them
        for jobgroup in jobgroup_bins.subbins:

            jobgroup_offset = y_postion + JOBGROUP_BIN_SIZE * jobgroup.pos + JOBGROUP_BIN_DIST
            jobgroup_height = JOBGROUP_BIN_SIZE - 2 * JOBGROUP_BIN_DIST
            jobgroup_width = (jobgroup.end - jobgroup.start)

            jobgroup_start_offset = jobgroup.start
            ax.add_patch(patches.Rectangle(
                (jobgroup_start_offset, jobgroup_offset), jobgroup_width, jobgroup_height, color=base_color, alpha=0.4
            ))

            jobgroup_endtime = jobgroup_start_offset + jobgroup.end - jobgroup.start
            ax.plot([jobgroup_endtime, jobgroup_endtime], [jobgroup_offset, jobgroup_offset + jobgroup_height],alpha=0.5, color='red', linestyle="-", linewidth=2)

            # for stage in jobgroup.subbins:
            #
            #     # to center the stages, add 1, so it ignores the ones on the edges of jobgroup
            #     stage_offset = jobgroup_offset + (jobgroup_height / (jobgroup.max + 1)) * (stage.pos + 1)
            #     stage_start_offset = stage.start
            #     stage_end_offset = stage.end
            #     ax.plot([stage_start_offset, stage_end_offset], [stage_offset,stage_offset], color='gray', linewidth=2)
            #     ax.scatter(stage_start_offset, stage_offset, color='green', s=10, zorder=2)  # Start marker
            #     ax.scatter(stage_end_offset, stage_offset, color='red', s=10, zorder=2)  # End marker

        # move the y position by the amount of overlapping jobgroup bins
        y_postion+= JOBGROUP_BIN_SIZE * jobgroup_bins.max






    # setup jobgroup plot
    ax.set_ylim(0, y_postion)
    # ax.set_xlim(0, total_time)

    ax.set_xlabel('Time (s)')
    ax.set_ylabel('Jobs')
    ax.set_yticks([]) 

    ax.grid(True, which='both', axis='x', linestyle='--', color='gray', alpha=0.5)
    fig.tight_layout()
    plt.show()


    os.makedirs(OUTPUT_DIR, exist_ok=True)
    fig.savefig(os.path.join(OUTPUT_DIR, f"macro_benchmark_timeline.{FIG_FORMAT}"))
    plt.close(fig)


def plot_congestion(user_list):


    jobgroup_bins = Bin(0, TIME_FRAME_S)

    total_runtime = 0
    for user_id, user in user_list.items():
        for jobgroup in user.workflow_id_to_job.values():
            # create bins for stages

            jobgroup_start, jobgroup_end = jobgroup.get_times()
            jobgroup_bin = Bin(jobgroup_start, jobgroup_end)
            jobgroup_bin.id= user_id
            # add jobgroup to its bin
            jobgroup_bins.add(jobgroup_bin)
            total_runtime += jobgroup_end - jobgroup_start

    print(f"total_runtime: {total_runtime}")
    jobgroup_bins.pack_subbins()


    users_per_second = []
    jobs_per_second = []
    for i in range(0, TIME_FRAME_S):

        users = jobgroup_bins.overlap(i, i + 1, on_id=True)
        jobs = jobgroup_bins.overlap(i, i + 1)
        users_per_second.append(users)
        jobs_per_second.append(jobs)

    
    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(10, 8), sharex=True)
    for ax in axes:
        ax.yaxis.set_major_locator(ticker.MultipleLocator(2))

    # First scatterplot
    x = np.arange(TIME_FRAME_S)
    axes[0].scatter(x, users_per_second, color='blue', alpha=0.6)
    axes[0].axhline(y=0, color='red', linestyle='--', linewidth=1, label='Reference Line at x=0')
    axes[0].set_ylabel('User amount')

    # Second scatterplot
    axes[1].scatter(x, jobs_per_second, color='green', alpha=0.6)
    axes[1].axhline(y=0, color='red', linestyle='--', linewidth=1, label='Reference Line at x=0')
    axes[1].set_xlabel('Time (s)')
    axes[1].set_ylabel('Jobs')


    fig.tight_layout()

    plt.show()
        

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    fig.savefig(os.path.join(OUTPUT_DIR, f"macro_benchmark_congestion.{FIG_FORMAT}"))
    plt.close(fig)



def make_bench_config(df):

    config = []
    for user_id, user in user_list.items():
        user_config = {}
        user_config["user"] = user.name
        workloads = []
        for jobgroup in user.workflow_id_to_job.values():
            jobgroup_start, jobgroup_end = jobgroup.get_times()
            for (start, end) in jobgroup.tasks:
                workload = {}
                workload["workloadName"] = f"{WORKLOAD_NAME}_{int(jobgroup.id)}"
                workload["inputPath"] = WORKLOAD_INPUT_PATH
                workload["inputType"] = WORKLOAD_INPUT_TYPE
                workload["className"] = WORKLOAD_CLASS
                workload["totalIterations"] = WORKLOAD_ITERATIONS
                workload["poissonRateInMinutes"] = WORKLOAD_RATE
                workload["frequency"] = WORKLOAD_FREQUENCY

                workload["startTimeMs"] = int(start * S_TO_MS)

                params = {}
                params["task_runtime_s"] = (end - start) * PARALLELIZATION_SCALING
                params["job_runtime_s"] = (jobgroup_end - jobgroup_start) * PARALLELIZATION_SCALING

                workload["params"] = params
                
                workloads.append(workload)
        
        user_config["workloads"] = workloads

        config.append(user_config)



    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filename = os.path.join(OUTPUT_DIR, f"macro_config.json")

    with open(filename, "w") as file:
        print("Saving data: " + filename)
        json.dump(config, file)



df = pd.read_csv(MACRO_CONFIG)


bench_start_time = min(df["ts_submit_seconds"])
print(f"bench start: {bench_start_time}")
user_list = parse_users(df, bench_start_time)



if FILTER_LARGE:
    filtered_user_list = {}
    job_list = [jobgroup.get_runtime() for user in user_list.values() for jobgroup in user.workflow_id_to_job.values()]
    med = np.median(job_list)
    cutoff = med * 5
    print(f"using median cuttoff: {cutoff}")

    for user_id, user in user_list.items():
        filter_user = User(user.name, user.bench_start_time)
        for jobgroup_id, jobgroup in user.workflow_id_to_job.items():
            if jobgroup.get_runtime() < cutoff:
                filter_user.workflow_id_to_job[jobgroup_id] = jobgroup
        filtered_user_list[user_id] = filter_user

    user_list = filtered_user_list

print_stats(user_list)

plot_histogram(user_list)
plot_timeline(user_list)
plot_congestion(user_list)
make_bench_config(user_list)


