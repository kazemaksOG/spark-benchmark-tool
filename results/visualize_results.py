from numpy import ones_like
from globals import *
from benchmark_classes import *
from utility import *
from latex_table_generator import *

import pandas as pd
import argparse
import json
import os
import pickle


import random
random.seed(42)

import matplotlib.pyplot as plt
import matplotlib.patches as patches

plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42


def create_table(args):

    benches = get_benchmarks(args.scheduler, args.config)

    
    run_rows = {}
    average_rows = {}
    for bench in benches:
        baseline_benches = None 
        if "PARTITIONER" in bench.scheduler:
            baseline_benches = get_benchmarks(args.compare_to + "-P", bench.config)
            print(f"Getting basline: {args.compare_to + "-P"}")
        else:
            baseline_benches = get_benchmarks(args.compare_to, bench.config)
            print(f"Getting basline: {args.compare_to}")
        current_scheduler = []
        for iteration, run in enumerate(bench.runs):
            print(f"Getting row elements for {run.app_name}, iteration: {iteration}")

            run_row = []

            # get start times
            start_time = min(jobgroup.start for user in run.users for jobgroup in user.jobgroups)
            end_time = max(jobgroup.end for user in run.users for jobgroup in user.jobgroups)

            job_amount = sum([1 for user in run.users for jobgroup in user.jobgroups])
            print(f"total jobgroup amount: job_amount")
            # general metrics
            total_time = (end_time - start_time)
            cpu_time = run.get_cpu_time() 
            write_ratio = run.get_write_ratio()
            total_task_scheduler_delay = sum(delay for user in run.users for jobgroup in user.jobgroups for delay in jobgroup.task_scheduler_delays)

            run_row.extend([
                ("Config", run.config),
                ("Scheduler", run.scheduler),
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
            




            sorted_rt = np.sort(all_rt)

            n = len(sorted_rt)
            percent80 = int(np.floor(0.8 * n ))
            percent95 = int(np.floor(0.95 * n ))

            top_80_avg = get_average(sorted_rt[:percent80])
            next_15_avg = get_average(sorted_rt[percent80:percent95])
            last_5_avg = get_average(sorted_rt[percent95:])




            run_row.extend([
                ("average response time", avg_rt),
                ("average response time (worst10%)", avg_rt_10),
                
                ("average response time (worst85)", top_80_avg),
                ("average response time (worst95)", next_15_avg),
                ("average response time (worst100)", last_5_avg),
                
                ("average proportional slowdown", proportional_slowdown_avg),
                ("average proportional slowdown (worst10%)", proportional_slowdown_avg_10),

                # ("average absolute slowdown", slowdown_avg),
                # ("average absolute slowdown (worst10%)", slowdown_avg_10),

            ])

            # per job type metrics
            for job_type in JOB_TYPES:
                jobs = [jobgroup.total_time for user in run.users for jobgroup in user.jobgroups if jobgroup.job_type == job_type]
                jobs_slowdown =  [jobgroup.slowdown for user in run.users for jobgroup in user.jobgroups if jobgroup.job_type == job_type]
                jobs_prop_slowdown =  [jobgroup.proportional_slowdown for user in run.users for jobgroup in user.jobgroups if jobgroup.job_type == job_type]


                if len(jobs) == 0:
                    continue


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
                    (f"{job_type} average proportional slowdown", jobs_prop_slowdown_avg),
                    (f"{job_type} average proportional slowdown (worst 10%)", jobs_prop_slowdown_avg_10),
                    # (f"{job_type} average absolute slowdown", jobs_slowdown_avg),

                    (f"{job_type} average response time (worst 1%)", jobs_rt_avg_1),

                    # (f"{job_type} average absolute slowdown (worst 10%)", jobs_slowdown_avg_10),
                    # (f"{job_type} average absolute slowdown (worst 1%)", jobs_slowdown_avg_1),

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
                    (f"{user.name} average proportional slowdown (worst 10%)", user_prop_slowdown_avg_10),
                    (f"{user.name} average response time (worst 10%)", user_rt_avg_10),
                    (f"{user.name} average proportional slowdown", user_prop_slowdown_avg),
                    # (f"{user.name} average absolute slowdown", user_slowdown_avg),

                    # (f"{user.name} average response time (worst 1%)", user_rt_avg_1),

                    # (f"{user.name} average absolute slowdown (worst 10%)", user_slowdown_avg_10),
                    # (f"{user.name} average absolute slowdown (worst 1%)", user_slowdown_avg_1),

                    # (f"{user.name} average proportional slowdown (worst 1%)", user_prop_slowdown_avg_1),
                ])

            # compare to fair scheduler
            deadline_violation = []
            deadline_slack = []
            baseline_run = (baseline_benches[0]).runs[0]
            job_deadline_violation = []
            job_deadline_slack = []


            start_time_base = min(base_jobgroup.start for user in baseline_run.users for base_jobgroup in user.jobgroups)
            for job_type in JOB_TYPES:
                for user in run.users:
                    if run.scheduler != args.compare_to:
                        for jobgroup in user.jobgroups:
                            if jobgroup.job_type != job_type:
                                continue



                            baseline_jobgroup = [base_jobgroup for user_base in baseline_run.users for base_jobgroup in user_base.jobgroups if base_jobgroup.name == jobgroup.name]
                            if len(baseline_jobgroup) == 0:
                                print("skipping: " + jobgroup.name + " for: " + baseline_run.scheduler)
                                continue

                            baseline_jobgroup = baseline_jobgroup[0]
                            # print("running: " + jobgroup.name + " for: " + run.scheduler)
                            # print(f"name: {baseline_jobgroup.name}")
                            # print(f"target: {jobgroup.total_time} base: {baseline_jobgroup.total_time}")


                            offset_target_end = jobgroup.end - start_time
                            offset_base_end = baseline_jobgroup.end - start_time_base
                            deadline_ratio = ((offset_target_end - offset_base_end)/ baseline_jobgroup.total_time) 
                            # print(f"target: {offset_target_end} base: {offset_base_end}")
                            # print(f"ratio: {deadline_ratio}")
                            if deadline_ratio > 0:
                                deadline_violation.append(deadline_ratio)
                                job_deadline_violation.append(deadline_ratio)
                            else:
                                deadline_slack.append(deadline_ratio)
                                job_deadline_slack.append(deadline_ratio)

                job_avg_deadline_miss = get_average(job_deadline_violation) 
                job_misses = len(job_deadline_violation)

                job_avg_deadline_gain = get_average(job_deadline_slack) 
                job_gains = len(job_deadline_slack)


                run_row.extend([
                    (f"{job_type} DVR", job_avg_deadline_miss),
                    (f"{job_type} violation count", job_misses),
                    (f"{job_type} DSR", job_avg_deadline_gain),
                    (f"{job_type} slack count", job_gains),
                ])


            worst_user_gain = None
            best_user_gain = None
            worst_user_miss = None
            best_user_miss = None
            for user in run.users:
                for jobgroup in user.jobgroups:

                    baseline_jobgroup = [base_jobgroup for user in baseline_run.users for base_jobgroup in user.jobgroups if base_jobgroup.name == jobgroup.name]
                    if len(baseline_jobgroup) == 0:
                        print("skipping: " + jobgroup.name + " for: " + baseline_run.scheduler)
                        continue

                    baseline_jobgroup = baseline_jobgroup[0]
                    # print("running: " + jobgroup.name + " for: " + run.scheduler)
                    # print(f"name: {baseline_jobgroup.name}")
                    # print(f"target: {jobgroup.total_time} base: {baseline_jobgroup.total_time}")

                    offset_target_end = jobgroup.end - start_time
                    offset_base_end = baseline_jobgroup.end - start_time_base
                    deadline_ratio = ((offset_target_end - offset_base_end)/ baseline_jobgroup.total_time) 
                    if deadline_ratio > 0:
                        deadline_violation.append(deadline_ratio)
                        job_deadline_violation.append(deadline_ratio)
                    else:
                        deadline_slack.append(deadline_ratio)
                        job_deadline_slack.append(deadline_ratio)

                job_avg_deadline_miss = get_average(job_deadline_violation) 

                job_avg_deadline_miss = get_average(job_deadline_violation) 
                job_misses = len(job_deadline_violation)

                job_avg_deadline_gain = get_average(job_deadline_slack) 
                job_gains = len(job_deadline_slack)

                if worst_user_gain == None or worst_user_gain > job_avg_deadline_gain:
                    worst_user_gain = job_avg_deadline_gain

                if worst_user_miss == None or worst_user_miss < job_avg_deadline_miss:
                    worst_user_miss = job_avg_deadline_miss

                if best_user_miss == None or best_user_miss > job_avg_deadline_miss:
                    best_user_miss = job_avg_deadline_miss

                if best_user_gain == None or best_user_gain < job_avg_deadline_gain:
                    best_user_gain = job_avg_deadline_gain

                run_row.extend([
                    (f"{user.name} DVR", job_avg_deadline_miss),
                    (f"{user.name} violation count", job_misses),
                    (f"{user.name} DSR", job_avg_deadline_gain),
                    (f"{user.name} slack count", job_gains),
                ])


            run_row.extend([
                (f"best user DVR", best_user_miss),
                (f"worst user DVR ", worst_user_miss),
                (f"best user DSR", best_user_gain),
                (f"worst user DSR", worst_user_gain),
            ])

                        
            avg_deadline_miss = get_average(deadline_violation) 
            deadline_miss = len(deadline_violation)

            avg_deadline_gain = get_average(deadline_slack) 
            deadline_gain = len(deadline_slack)

            run_row.extend([

                ("Total DVR", avg_deadline_miss),
                ("Total violations", deadline_miss),

                ("Total DSR", avg_deadline_gain),
                ("Total gains", deadline_gain),
            ])



            # append the row
            if run.config not in run_rows.keys():
                run_rows[run.config] = []

            current_scheduler.append(run_row)
            run_rows[run.config].append(run_row)


        average_row = []
        for index, tup in enumerate(current_scheduler[0]):
            title = tup[0]
            average = 0
            for run in current_scheduler:
                value = run[index][1]
                if isinstance(value, str) or value is None:
                    average = value 
                    break
                average += value / len(current_scheduler)
            row = (title, average)
            average_row.append(row)


        if bench.config not in average_rows.keys():
            average_rows[bench.config] = []
        average_rows[bench.config].append(average_row)
        print(f"finished: bench.app_name")

    for config in CONFIGS:
        if config not in run_rows.keys():
            continue
        run_data = run_rows[config]
        average_data = average_rows[config]
        columns = [col[0] for col in run_data[0]]
        values = [[val[1] for val in row] for row in run_data]
        average_values = [[val[1] for val in row] for row in average_data]
        df = pd.DataFrame(values, columns=columns)
        df_avg = pd.DataFrame(average_values, columns=columns)

        # df = df.sort_values(by=["Config", "Scheduler"])

        # make a dir if necessary
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        df.to_csv(os.path.join(OUTPUT_DIR, f"{config}_run_data.csv"))
        df_avg.to_csv(os.path.join(OUTPUT_DIR, f"{config}_run_data_avg.csv"))

        if config == "2_large_2_small_users":
            latex_2_large_2_small_users(df_avg)
        elif config == "4_large_users":
            latex_4_large_users(df_avg)
        elif config == "2_power_2_small_users":
            latex_2_power_2_small_users(df_avg)
        elif config == "4_super_small_users":
            latex_4_super_small_users(df_avg)
        elif config == "hetero_macro":
            latex_hetero_macro(df_avg)
        elif config == "homo_macro":
            latex_homo_macro(df_avg)





def boxplot_deadline(args):

    benches = get_benchmarks(args.scheduler, args.config)

    



    for config in CONFIGS:
        fig_default, ax_default = plt.subplots()
        labels_default = []
        #labels_partition = []
        fig_partition, ax_partition = plt.subplots()
        labels = []
        empty = True
        for index, bench in enumerate(benches):
            if bench.config != config:
                continue

            baseline_benches = None 
            ax = None

            labels = labels_default
            ax = ax_default
            if "PARTITIONER" in bench.scheduler:
                baseline_benches = get_benchmarks(args.compare_to + "-P", bench.config)
                continue
                # labels = labels_partition
                # ax = ax_partition
            else:
                baseline_benches = get_benchmarks(args.compare_to, bench.config)
                # labels = labels_default
                # ax = ax_default

            deadline_violation = []
            deadline_slack = []
            for iteration, run in enumerate(bench.runs):

                if FORMAL_NAME[run.scheduler] == args.compare_to or FORMAL_NAME[run.scheduler] == args.compare_to + "-P":
                    continue

                # get start times
                start_time = min(jobgroup.start for user in run.users for jobgroup in user.jobgroups)

                # compare to fair scheduler
                baseline_run = (baseline_benches[0]).runs[0]

                start_time_base = min(base_jobgroup.start for user in baseline_run.users for base_jobgroup in user.jobgroups)
                for user in run.users:
                    if run.scheduler != args.compare_to:
                        for jobgroup in user.jobgroups:
                            baseline_jobgroup = [base_jobgroup for user_base in baseline_run.users for base_jobgroup in user_base.jobgroups if base_jobgroup.name == jobgroup.name]
                            if len(baseline_jobgroup) == 0:
                                print("skipping: " + jobgroup.name + " for: " + baseline_run.scheduler)
                                continue

                            baseline_jobgroup = baseline_jobgroup[0]
                            # print("running: " + jobgroup.name + " for: " + run.scheduler)
                            # print(f"name: {baseline_jobgroup.name}")
                            # print(f"target: {jobgroup.total_time} base: {baseline_jobgroup.total_time}")


                            offset_target_end = jobgroup.end - start_time
                            offset_base_end = baseline_jobgroup.end - start_time_base
                            deadline_ratio = ((offset_target_end - offset_base_end)/ baseline_jobgroup.total_time) 
                            # print(f"target: {offset_target_end} base: {offset_base_end}")
                            # print(f"ratio: {deadline_ratio}")
                            if deadline_ratio > 0:
                                deadline_violation.append(deadline_ratio)
                            else:
                                deadline_slack.append(deadline_ratio)

                if FORMAL_NAME[run.scheduler] == "UWFQ" or FORMAL_NAME[run.scheduler] == "UWFQ-P" :
                    labels.append(r'$\mathbf{{{}}}$'.format(FORMAL_NAME[run.scheduler]))
                else:
                    labels.append(FORMAL_NAME[run.scheduler])
                print(f"done {run.scheduler}, {run.config}, with amount of deadlines: {len(deadline_violation + deadline_slack)}")
                empty = False
                break
    
            position = len(labels) - 1
            ax.boxplot(deadline_violation + deadline_slack, positions=[position], widths=0.6)            
            if "-P" in labels[len(labels) - 1]:
                ax.axvline(x=position + 0.5, color='gray', linestyle='--', linewidth=1)



        if empty:
            continue

        ax_default.set_xlabel("Scheduler")
        ax_default.set_ylabel("Job proportional slack/violation")
        ax_default.set_xticks(range(len(labels_default)))
        ax_default.set_xticklabels(labels_default, rotation=45, ha='right')
        ax_default.axhline(y=0, color='red', linestyle='--', linewidth=1)

        plt.tight_layout()
        if args.show_plot:
            plt.show()


        # make a dir if necessary
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        filename = os.path.join(OUTPUT_DIR, f"{config}_deadline_boxplots.{FIG_FORMAT}") 
        print(f"saving {filename}")
        fig_default.savefig(filename, bbox_inches='tight')
        plt.close(fig_default)



        # ax_partition.set_xticks(range(len(labels_partition)))
        # ax_partition.set_xticklabels(labels_partition, rotation=45, ha='right')
        # ax_partition.axhline(y=0, color='red', linestyle='--', linewidth=1)
        # plt.tight_layout()
        #
        # if args.show_plot:
        #     plt.show()
        #
        # # make a dir if necessary
        # os.makedirs(OUTPUT_DIR, exist_ok=True)
        #
        # filename = os.path.join(OUTPUT_DIR, f"{config}_deadline_boxplots_partition.{FIG_FORMAT}") 
        # print(f"saving {filename}")
        # fig_partition.savefig(filename, bbox_inches='tight')
        # plt.close(fig_partition)


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

                
    elif args.change_type == "custom1":
        for config in CONFIGS:
            # for job_type in JOB_TYPES:
                fig_default, ax_default = plt.subplots()
                fig_partition, ax_partition = plt.subplots()
                empty = True
                for bench in benches:
                    if config not in bench.config:
                        continue

                    fig = None 
                    ax = None
                    if "PARTITIONER" in bench.scheduler:
                        fig = fig_partition
                        ax = ax_partition
                    else:
                        fig = fig_default
                        ax = ax_default
                    # if bench.scheduler not in args.compare_to:
                    #     continue
                    for run in bench.runs:


                        all_rt = [jobgroup.total_time for user in run.users for jobgroup in user.jobgroups]

                        # job_rt = [jobgroup.total_time for user in run.users for jobgroup in user.jobgroups if "user4" in user.name ]

                        # if no such job for this benchmark, continue
                        if len(all_rt) == 0:
                            continue

                        # no need to cmpare baseline to itself
                        print(f"using schedule: {run.scheduler}")
                        # get data for baseline

                        # plot data

                        label = None
                        if FORMAL_NAME[run.scheduler] == "UWFQ" or FORMAL_NAME[run.scheduler] == "UWFQ-P" :
                            label = (r'$\mathbf{{{}}}$'.format(FORMAL_NAME[run.scheduler]))
                        else:
                            label = (FORMAL_NAME[run.scheduler])

                        ax.ecdf(all_rt, label=label, linestyle=SCHEDULER_LINE[run.scheduler], color=SCHEDULER_COLOR[run.scheduler])
                        empty = False
                        break


                if empty:
                    continue

                ax_default.grid(True)
                ax_default.legend()
                # ax_default.set_title(f"Response time ECDF :{run.config}")
                ax_default.set_xlabel("Response time (s)")
                ax_default.set_ylabel("Fraction of jobs")
                ax_default.set_ylim(ymin=0)
                ax_default.set_xlim(xmin=0)

                if args.show_plot:
                    plt.show()


                os.makedirs(OUTPUT_DIR, exist_ok=True)

                filename = os.path.join(OUTPUT_DIR, f"{config}_all_ecdf.{FIG_FORMAT}")
                print(f"saving {filename}")
                fig_default.savefig(filename)
                plt.close(fig_default)



                ax_partition.grid(True)
                ax_partition.legend()
                # ax_partition.set_title(f"Response time ECDF :{run.config}")
                ax_partition.set_xlabel("Response time (s)")
                ax_partition.set_ylabel("Fraction of jobs")
                ax_partition.set_ylim(ymin=0)
                ax_partition.set_xlim(xmin=0)

                if args.show_plot:
                    plt.show()


                os.makedirs(OUTPUT_DIR, exist_ok=True)

                filename = os.path.join(OUTPUT_DIR, f"{config}_all_ecdf_partition.{FIG_FORMAT}")
                print(f"saving {filename}")
                fig_partition.savefig(filename)
                plt.close(fig_partition)


    elif args.change_type == "custom2":
        for config in CONFIGS:
            # for job_type in JOB_TYPES:

                fig_mini, ax_mini = plt.subplots()
                fig_short, ax_short = plt.subplots()
                fig_med, ax_med = plt.subplots()
                fig_long, ax_long = plt.subplots()
                empty = True
                for bench in benches:
                    if config not in bench.config:
                        continue
                    if "PARTITIONER" in bench.scheduler:
                        print(f"skipping: {bench.scheduler}")
                        continue
                    if FORMAL_NAME[bench.scheduler] not in args.compare_to:
                        print(f"skipping: {bench.scheduler}")
                        continue
                    for run in bench.runs:



                        all_rt = [jobgroup.total_time for user in run.users for jobgroup in user.jobgroups ]

                        # if no such job for this benchmark, continue
                        if len(all_rt) == 0:
                            continue

                        # no need to cmpare baseline to itself
                        print(f"using schedule: {run.scheduler}")
                        # get data for baseline

                        sorted_rt = np.sort(all_rt)

                        n = len(sorted_rt)
                        percent20 = int(np.floor(0.2 * n))
                        percent80 = int(np.floor(0.8 * n ))
                        percent95 = int(np.floor(0.95 * n ))

                        top_20 = sorted_rt[:percent20]
                        top_80 = sorted_rt[:percent80]
                        next_15 = sorted_rt[percent80:percent95]
                        last_5 = sorted_rt[percent95:]
                        

                        label = None
                        if FORMAL_NAME[run.scheduler] == "UWFQ" or FORMAL_NAME[run.scheduler] == "UWFQ-P" :
                            label = (r'$\mathbf{{{}}}$'.format(FORMAL_NAME[run.scheduler]))
                        else:
                            label = (FORMAL_NAME[run.scheduler])

                        ax_mini.ecdf(top_20, label=label, linestyle=SCHEDULER_LINE[run.scheduler], color=SCHEDULER_COLOR[run.scheduler])
                        ax_short.ecdf(top_80, label=label, linestyle=SCHEDULER_LINE[run.scheduler], color=SCHEDULER_COLOR[run.scheduler])
                        ax_med.ecdf(next_15, label=label, linestyle=SCHEDULER_LINE[run.scheduler], color=SCHEDULER_COLOR[run.scheduler])
                        ax_long.ecdf(last_5, label=label, linestyle=SCHEDULER_LINE[run.scheduler], color=SCHEDULER_COLOR[run.scheduler])

                        empty = False
                        break


                if empty:
                    continue



                ax_mini.grid(True)
                ax_mini.legend()
                # ax_mini.set_title(f"Response time ECDF :{run.config}")
                ax_mini.set_xlabel("Response time (s)")
                ax_mini.set_ylabel("Fraction of jobs")
                ax_mini.set_ylim(ymin=0)
                ax_mini.set_xlim(xmin=0)

                if args.show_plot:
                    plt.show()


                os.makedirs(OUTPUT_DIR, exist_ok=True)

                filename = os.path.join(OUTPUT_DIR, f"{config}_mini_ecdf_partition.{FIG_FORMAT}")
                print(f"saving {filename}")
                fig_mini.savefig(filename)
                plt.close(fig_mini)


                ax_short.grid(True)
                ax_short.legend()
                # ax_short.set_title(f"Response time ECDF :{run.config}")
                ax_short.set_xlabel("Response time (s)")
                ax_short.set_ylabel("Fraction of jobs")
                ax_short.set_ylim(ymin=0)
                ax_short.set_xlim(xmin=0)

                if args.show_plot:
                    plt.show()


                os.makedirs(OUTPUT_DIR, exist_ok=True)

                filename = os.path.join(OUTPUT_DIR, f"{config}_short_ecdf_partition.{FIG_FORMAT}")
                print(f"saving {filename}")
                fig_short.savefig(filename)
                plt.close(fig_short)



                ax_med.grid(True)
                ax_med.legend()
                # ax_med.set_title(f"Response time ECDF :{run.config}")
                ax_med.set_xlabel("Response time (s)")
                ax_med.set_ylabel("Fraction of jobs")
                ax_med.set_ylim(ymin=0)
                ax_med.set_xlim(xmin=0)

                if args.show_plot:
                    plt.show()


                os.makedirs(OUTPUT_DIR, exist_ok=True)

                filename = os.path.join(OUTPUT_DIR, f"{config}_med_ecdf_partition.{FIG_FORMAT}")
                print(f"saving {filename}")
                fig_med.savefig(filename)
                plt.close(fig_med)




                ax_long.grid(True)
                ax_long.legend()
                # ax_long.set_title(f"Response time ECDF :{run.config}")
                ax_long.set_xlabel("Response time (s)")
                ax_long.set_ylabel("Fraction of jobs")
                ax_long.set_ylim(ymin=0)
                ax_long.set_xlim(xmin=0)

                if args.show_plot:
                    plt.show()


                os.makedirs(OUTPUT_DIR, exist_ok=True)

                filename = os.path.join(OUTPUT_DIR, f"{config}_long_ecdf_partition.{FIG_FORMAT}")
                print(f"saving {filename}")
                fig_long.savefig(filename)
                plt.close(fig_long)


    elif args.change_type == "custom3":
        for config in CONFIGS:
            for job_type in JOB_TYPES:
                fig_default, ax_default = plt.subplots()
                fig_partition, ax_partition = plt.subplots()
                empty = True
                for bench in benches:
                    if config not in bench.config:
                        continue

                    fig = None 
                    ax = None
                    if "PARTITIONER" in bench.scheduler:
                        fig = fig_partition
                        ax = ax_partition
                    else:
                        fig = fig_default
                        ax = ax_default

                    if FORMAL_NAME[bench.scheduler] not in args.compare_to:
                        continue
                    for run in bench.runs:



                        job_rt = [jobgroup.total_time for user in run.users for jobgroup in user.jobgroups if jobgroup.job_type in job_type]

                        # if no such job for this benchmark, continue
                        if len(job_rt) == 0:
                            continue

                        # no need to cmpare baseline to itself
                        print(f"using schedule: {run.scheduler}")
                        # get data for baseline

                        # plot data

                        label = None
                        if FORMAL_NAME[run.scheduler] == "UWFQ" or FORMAL_NAME[run.scheduler] == "UWFQ-P" :
                            label = (r'$\mathbf{{{}}}$'.format(FORMAL_NAME[run.scheduler]))
                        else:
                            label = (FORMAL_NAME[run.scheduler])

                        ax.ecdf(job_rt, label=label, linestyle=SCHEDULER_LINE[run.scheduler], color=SCHEDULER_COLOR[run.scheduler])
                        empty = False
                        break


                if empty:
                    continue

                ax_default.grid(True)
                ax_default.legend()
                # ax_default.set_title(f"Response time ECDF :{run.config}")
                ax_default.set_xlabel("Response time (s)")
                ax_default.set_ylabel("Fraction of jobs")
                ax_default.set_ylim(ymin=0)
                ax_default.set_xlim(xmin=0)

                if args.show_plot:
                    plt.show()


                os.makedirs(OUTPUT_DIR, exist_ok=True)

                filename = os.path.join(OUTPUT_DIR, f"{config}_{job_type}ecdf.{FIG_FORMAT}")
                print(f"saving {filename}")
                fig_default.savefig(filename)
                plt.close(fig_default)



                ax_partition.grid(True)
                ax_partition.legend()
                # ax_partition.set_title(f"Response time ECDF :{run.config}")
                ax_partition.set_xlabel("Response time (s)")
                ax_partition.set_ylabel("Fraction of jobs")
                ax_partition.set_ylim(ymin=0)
                ax_partition.set_xlim(xmin=0)

                if args.show_plot:
                    plt.show()


                os.makedirs(OUTPUT_DIR, exist_ok=True)

                filename = os.path.join(OUTPUT_DIR, f"{config}_{job_type}ecdf_partition.{FIG_FORMAT}")
                print(f"saving {filename}")
                fig_partition.savefig(filename)
                plt.close(fig_partition)

    elif args.change_type == "custom4":
        comp_user = "user4"
        for config in CONFIGS:
                fig_default, ax_default = plt.subplots()
                empty = True
                for bench in benches:
                    if config not in bench.config:
                        continue

                    if "PARTITIONER"  in bench.scheduler:
                        continue
                    fig = fig_default
                    ax = ax_default


                    if FORMAL_NAME[bench.scheduler] not in args.compare_to:
                        continue
                    for run in bench.runs:



                        job_rt = [jobgroup.total_time for user in run.users for jobgroup in user.jobgroups if comp_user in user.name ]

                        # if no such job for this benchmark, continue
                        if len(job_rt) == 0:
                            continue

                        # no need to cmpare baseline to itself
                        print(f"using schedule: {run.scheduler}")
                        # get data for baseline

                        # plot data

                        label = None
                        if FORMAL_NAME[run.scheduler] == "UWFQ" or FORMAL_NAME[run.scheduler] == "UWFQ-P" :
                            label = (r'$\mathbf{{{}}}$'.format(FORMAL_NAME[run.scheduler]))
                        else:
                            label = (FORMAL_NAME[run.scheduler])

                        ax.ecdf(job_rt, label=label, linestyle=SCHEDULER_LINE[run.scheduler], color=SCHEDULER_COLOR[run.scheduler])
                        empty = False
                        break


                if empty:
                    continue

                ax_default.grid(True)
                ax_default.legend()
                # ax_default.set_title(f"Response time ECDF :{run.config}")
                ax_default.set_xlabel("Response time (s)")
                ax_default.set_ylabel("Fraction of jobs")
                ax_default.set_ylim(ymin=0)
                ax_default.set_xlim(xmin=0)

                if args.show_plot:
                    plt.show()


                os.makedirs(OUTPUT_DIR, exist_ok=True)

                filename = os.path.join(OUTPUT_DIR, f"{config}_{comp_user}_ecdf.{FIG_FORMAT}")
                print(f"saving {filename}")
                fig_default.savefig(filename)
                plt.close(fig_default)









def timeline(args):

    benches = get_benchmarks(args.scheduler, args.config)

    
    for bench in benches:
        for iteration, run in enumerate(bench.runs):
            print(f"Making timeline for {run.app_name}, iteration {iteration}")
            fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(10, 8), sharex=True)

            if not TASK_TRACKING_ENABLED:
                plt.close(fig)
                fig, ax = plt.subplots(figsize=(10, 4))

                axes = [None, ax]

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
            if TASK_TRACKING_ENABLED:
                executor_bins_map = {}
                for i in range(EXECUTOR_AMOUNT):
                    executor_bins_map[i] = Bin(0,0)
            for user in run.users:
                base_color = user_colors[user.name]
                jobgroup_bins = Bin(start_time, end_time)
                for jobgroup in user.jobgroups:
                    if TASK_TRACKING_ENABLED:
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




            if TASK_TRACKING_ENABLED:
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
            save_dir = os.path.join(OUTPUT_DIR, f"{run.config}")
            os.makedirs(save_dir, exist_ok=True)

            filename = os.path.join(save_dir, f"{run.scheduler}_{run.iteration}_user_job_timeline.{FIG_FORMAT}")

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
            filtered = [bench for bench in benches if isolate_config in bench.config and (isolate_scheduler == "" or isolate_scheduler == FORMAL_NAME[bench.scheduler])]
            filtered.sort(key=lambda bench: SCHEDULER_ORDER.index(bench.scheduler))
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
            if scheduler == "" or config == "":
                continue
            tup = (scheduler, config)
            if isolate_scheduler in scheduler and isolate_config in config:
                if tup not in unique_bench:
                    users = get_bench_users(file_path, base_runtimes)
                    benches.append(Benchmark(scheduler, config, users))
                    unique_bench.append(tup)

    # if no benches are found, infer from Spark API
    if len(benches) == 0:
        for scheduler in SCHEDULERS:
            for config in CONFIGS:
                bench = Benchmark(scheduler, config)
                if len(bench.runs) != 0:
                    benches.append(bench)



    # save data for future runs
    with open(data_dump_path, "wb") as file:
        print("Saving data: " + data_dump_path)
        pickle.dump(benches, file)

                    
    benches.sort(key=lambda bench: SCHEDULER_ORDER.index(bench.scheduler))
    return benches






if __name__ == "__main__":
    parser = argparse.ArgumentParser("Show certain benchmark results")
    parser.add_argument("--scheduler", help="Scheduler to isolate", action="store", default="")
    parser.add_argument("--config", help="Config to isolate", action="store", default="")
    parser.add_argument("--show_plot", help="Shows the plot using matplotlib GUI", action="store_true", default=False)

    subparsers = parser.add_subparsers(title="Commands")

    create_table_parser = subparsers.add_parser("create_table", help="Create an excel table that summerizes all results")
    create_table_parser.add_argument("--compare_to", help="Scheduler to be taken as base", default="UJF" )
    create_table_parser.set_defaults(func=create_table)

    timeline_parser = subparsers.add_parser("timeline", help="Create event timeline images")

    timeline_parser.add_argument("--show_stage", help="Show stage in timeline", action="store_true", default=False)
    timeline_parser.add_argument("--show_stage_id", help="Show stage id in timeline", action="store_true", default=False)
    timeline_parser.add_argument("--show_task_stage_id", help="Show stage id on tasks", action="store_true", default=False)
    timeline_parser.add_argument("--change_type", help="Show tasks in certain type. Values: type, group", action="store", default="group")
    timeline_parser.set_defaults(func=timeline)


    cdf_parser = subparsers.add_parser("cdf", help="Create ECDFs of response time")
    cdf_parser.add_argument("--change_type", help="Show different type of cdf metrics. Values: user, job, total, custom", default="total")
    cdf_parser.add_argument("--compare_to", help="Scheduler to be taken as base", default="UJF" )
    cdf_parser.set_defaults(func=cdf)



    boxplot_parser = subparsers.add_parser("boxplots", help="Create boxplots of deadline violations")
    # cdf_parser.add_argument("--change_type", help="Show different type of cdf metrics. Values: user, job, total, custom", default="total")
    boxplot_parser.add_argument("--compare_to", help="Scheduler to be taken as base", default="UJF" )
    boxplot_parser.set_defaults(func=boxplot_deadline)

    # unfairness_parser = subparsers.add_parser("unfairness", help="Create unfairness boxplots")
    # unfairness_parser.add_argument("--change_type", help="Show different type of unfairness metrics. Values: user, proportional, absolute", default="user")
    # unfairness_parser.set_defaults(func=unfairness)

    
    args = parser.parse_args()
    args.func(args)
   







