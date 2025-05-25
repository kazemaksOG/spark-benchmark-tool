from globals import *
from benchmark_classes import *
from utility import *

import pandas as pd
import argparse
import json
import os
import pickle


import random
random.seed(42)

import matplotlib.pyplot as plt
import matplotlib.patches as patches


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


            all_rt = [jobgroup.total_time for user in run.users for jobgroup in user.jobgroups]
            avg_rt = get_average(all_rt)
            avg_rt_10 = get_worst_10_percent(all_rt)
            slowdown_avg = get_average(all_slowdowns) 
            slowdown_avg_10 = get_worst_10_percent(all_slowdowns) 
            proportional_slowdown_avg = get_average(all_proportional_slowdowns)
            proportional_slowdown_avg_10 = get_worst_10_percent(all_proportional_slowdowns)
            

            run_row.extend([
                ("average response time", avg_rt),
                ("average response time (worst10%)", avg_rt_10),
                
                ("average proportional slowdown", proportional_slowdown_avg),
                ("average proportional slowdown (worst10%)", proportional_slowdown_avg_10),

                ("average absolute slowdown", slowdown_avg),
                ("average absolute slowdown (worst10%)", slowdown_avg_10),

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
                    (f"{job_type} average proportional slowdown", jobs_prop_slowdown_avg),
                    (f"{job_type} average absolute slowdown", jobs_slowdown_avg),

                    (f"{job_type} average response time (worst 10%)", jobs_rt_avg_10),
                    (f"{job_type} average response time (worst 1%)", jobs_rt_avg_1),

                    (f"{job_type} average absolute slowdown (worst 10%)", jobs_slowdown_avg_10),
                    (f"{job_type} average absolute slowdown (worst 1%)", jobs_slowdown_avg_1),

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
                    (f"{user.name} average proportional slowdown", user_prop_slowdown_avg),
                    (f"{user.name} average absolute slowdown", user_slowdown_avg),

                    (f"{user.name} average response time (worst 10%)", user_rt_avg_10),
                    (f"{user.name} average response time (worst 1%)", user_rt_avg_1),

                    (f"{user.name} average absolute slowdown (worst 10%)", user_slowdown_avg_10),
                    (f"{user.name} average absolute slowdown (worst 1%)", user_slowdown_avg_1),

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
                            # print("running: " + jobgroup.name + " for: " + run.scheduler)
                            # print(f"name: {baseline_jobgroup.name}")
                            # print(f"target: {jobgroup.total_time} base: {baseline_jobgroup.total_time}")
                            if jobgroup.total_time > baseline_jobgroup.total_time:
                                deadline_miss.append(jobgroup.total_time - baseline_jobgroup.total_time)
                                job_deadline_miss.append(jobgroup.total_time - baseline_jobgroup.total_time)
                            else:
                                deadline_gain.append(baseline_jobgroup.total_time - jobgroup.total_time)
                                job_deadline_gain.append(baseline_jobgroup.total_time - jobgroup.total_time)

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
                            (f"{job_type} Total gained deadline time", job_sum_deadline_gain),

                            (f"{job_type} Average missed deadline time", job_avg_deadline_miss),
                            (f"{job_type} Worst 10% missed deadline time", job_avg_10_deadline_miss),
                            (f"{job_type} Worst 1% missed deadline time", job_avg_1_deadline_miss),

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
                ("Total gained deadline time", sum_deadline_gain),

                ("Average missed deadline time", avg_deadline_miss),
                ("Worst 10% missed deadline time", avg_10_deadline_miss),
                ("Worst 1% missed deadline time", avg_1_deadline_miss),

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
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        df.to_csv(os.path.join(OUTPUT_DIR, f"{bench.scheduler}_{bench.config}_run_data.csv"))





def plot_and_save_cdf(target, baseline, target_name, base_name, folder, output):
    # plot data
    fig, ax = plt.subplots()

    ax.ecdf(target, label=f"{FORMAL_NAME[target_name]}", linestyle=SCHEDULER_LINE[target_name], color=SCHEDULER_COLOR[target_name])

    ax.ecdf(baseline, label=f"{FORMAL_NAME[base_name]}", linestyle=SCHEDULER_LINE[base_name], color=SCHEDULER_COLOR[base_name])


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

    filename = os.path.join(folder, f"{output}.{FIG_FORMAT}") 
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

                plot_and_save_cdf(all_rt, baseline_rt, run.scheduler, args.compare_to, os.path.join(OUTPUT_DIR,f"{run.scheduler}_{run.config}"), "overall_rt")


                

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

                    plot_and_save_cdf(user_rt, baseline_user_rt, run.scheduler, args.compare_to, os.path.join(OUTPUT_DIR,f"{run.scheduler}_{run.config}"), f"{user.name}_response_time_cdf")

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

                    plot_and_save_cdf(job_rt, baseline_job_rt, run.scheduler, args.compare_to, os.path.join(OUTPUT_DIR, f"{run.scheduler}_{run.config}"),f"{job_type}_response_time_cdf")

                
    elif args.change_type == "custom":
        for configuration in CONFIGS:
            fig, ax = plt.subplots()
            for bench in benches:
                if configuration != bench.config:
                    continue
                if "PARTITIONER" not in bench.scheduler and "DEFAULT" not in bench.scheduler:
                    continue
                for run in bench.runs:

                    # no need to cmpare baseline to itself
                    if run.scheduler not in args.compare_to:
                        continue
                    print(f"using schedule: {run.scheduler}")

                    all_rt = [jobgroup.total_time for user in run.users for jobgroup in user.jobgroups]

                    # plot data

                    ax.ecdf(all_rt, label=f"{FORMAL_NAME[run.scheduler]}", linestyle=SCHEDULER_LINE[run.scheduler], color=SCHEDULER_COLOR[run.scheduler])



            ax.grid(True)
            ax.legend()
            # ax.set_title(f"Response time ECDF :{run.config}")
            ax.set_xlabel("Response time (s)")
            ax.set_ylabel("Fraction of jobs")
            ax.set_ylim(ymin=0)
            ax.set_xlim(xmin=0)

            if args.show_plot:
                plt.show()


            os.makedirs(OUTPUT_DIR, exist_ok=True)

            filename = os.path.join(OUTPUT_DIR, f"{configuration}_custom_ecdf.{FIG_FORMAT}")
            print(f"saving {filename}")
            fig.savefig(filename)
            plt.close(fig)







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
            os.makedirs(OUTPUT_DIR, exist_ok=True)

            filename = os.path.join(OUTPUT_DIR, f"{run.scheduler}_{run.config}_user_job_timeline.{FIG_FORMAT}")

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
    cdf_parser.add_argument("--change_type", help="Show different type of cdf metrics. Values: user, job, total, custom", default="total")
    cdf_parser.add_argument("--compare_to", help="Scheduler to be taken as base", default="DEFAULT_FAIR" )
    cdf_parser.set_defaults(func=cdf)


    # unfairness_parser = subparsers.add_parser("unfairness", help="Create unfairness boxplots")
    # unfairness_parser.add_argument("--change_type", help="Show different type of unfairness metrics. Values: user, proportional, absolute", default="user")
    # unfairness_parser.set_defaults(func=unfairness)

    
    args = parser.parse_args()
    args.func(args)
   







