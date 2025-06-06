from globals import *
from utility import *

import requests
import copy
from datetime import datetime

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
            print(f"Parsing {app_id}")
            run = Run(self.scheduler, self.config, copy.deepcopy(self.users_template), i)
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
        if TASK_TRACKING_ENABLED:
            return sum([execution.total_time for user in self.users for jobgroup in user.jobgroups for execution in jobgroup.executor_load])
        else:
            return 0
    def get_write_ratio(self):
        if TASK_TRACKING_ENABLED:
            write_time = sum([execution.total_time for user in self.users for jobgroup in user.jobgroups for execution in jobgroup.executor_load if execution.ex_type == "WRITE"])
            return write_time / self.get_cpu_time()
        else:
            return 0

    def get_event_data(self, app_id):
        
        # get executor details

        print(f"Getting executor data...")
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

        print(f"Done executor data")

        # get user and more detailed execution data 
        stage_response = requests.get(f"{APPS_URL}/{app_id}/stages")
        if stage_response.status_code != 200:
            raise Exception(f"Couldnt get all stages: {APPS_URL}/{app_id}/stages")

        stages_json = stage_response.json()

        id_correction = {}
        for stage_json in stages_json:
            id_correction[stage_json["stageId"]] = stage_json


        job_response = requests.get(f"{APPS_URL}/{app_id}/jobs")
        if job_response.status_code != 200:  # Check if the request was successful
            raise Exception(f"Couldnt get all jobs: {APPS_URL}/{app_id}/jobs")
        jobs_json = job_response.json()  # Convert response to JSON

        id_correction = {}

        for job_json in jobs_json:
            id_correction[job_json["jobId"]] = job_json
        jobs_json = id_correction

        for user in self.users:
            user.get_event_data(app_id, stages_json, jobs_json)



    

class User:
    def __init__(self, name, base_runtimes):
        self.name = name + "_" # to avoid misassociations
        self.base_runtimes = base_runtimes
        self.jobgroups = []



    def get_event_data(self, app_id, stages_json, jobs_json):
        print(f"Going over jobs in {self.name} ...")
            # find the jobs for data

        user_jobgroup_map = {}
        for job in jobs_json.values():
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
            jobgroup.get_event_data(app_id, jobs_json, stages_json)
            self.jobgroups.append(jobgroup)
                

        print(f"Done with all jobs for user {self.name}")




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

    def get_event_data(self, app_id, jobs_json, stages_json):

        for job_id in self.jobs:

            job_json = jobs_json[job_id]

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
                # returns an array of one element
                stage_json = stages_json[stage_id]
                if stage_json["status"] == "COMPLETE":
                    # convert times to seconds since epoch
                    stage_start = stage_json["firstTaskLaunchedTime"]
                    stage_start_dt = datetime.strptime(stage_start[:-3], "%Y-%m-%dT%H:%M:%S.%f")
                    stage_start_s= stage_start_dt.timestamp()

                    stage_end = stage_json["completionTime"]
                    stage_end_dt = datetime.strptime(stage_end[:-3], "%Y-%m-%dT%H:%M:%S.%f")
                    stage_end_s= stage_end_dt.timestamp()
                    self.stages.append(Stage(stage_id, stage_start_s, stage_end_s))


                    if TASK_TRACKING_ENABLED:
                        # get how much time spent on executor
                        response = requests.get(f"{APPS_URL}/{app_id}/stages/{stage_id}")
                        if response.status_code != 200:
                            print(f"Stage {stage_id} was either skipped or failed (or something else) for job {job_id}, response: {response.status_code}") 
                            continue
                        stage_json = response.json()[0]
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


