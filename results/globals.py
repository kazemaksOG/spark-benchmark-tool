

# result parsing settings

EXECUTOR_AMOUNT = 8
CORES_PER_EXEC = 4
CORES = EXECUTOR_AMOUNT * CORES_PER_EXEC

TIME_FRAME_S = 500
SCALING = 2

PARALLELIZATION_SCALING = 30

FIG_FORMAT = "png"

RUN_PATH="./data/microbench_5/target"
BENCH_PATH=f"{RUN_PATH}/bench_outputs"
OUTPUT_DIR="./metrics"

# history server address
APPS_URL="http://localhost:18080/api/v1/applications"


SCHEDULERS = [
    "CUSTOM_FAIR",
    "CUSTOM_RANDOM",
    "CUSTOM_SHORT",
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
    "CUSTOM_USERCLUSTERFAIR": "UWFQ",
    "CUSTOM_USERCLUSTERFAIR_PARTITIONER": "UWFQ-P",
    "CUSTOM_RANDOM": "Random",
    "CUSTOM_SHORT": "SJF",
    "CUSTOM_FAIR": "User Fair",
}


SCHEDULER_COLOR = {
    "DEFAULT_FAIR": "tab:blue",
    "DEFAULT_FAIR_PARTITIONER": "tab:orange",
    "CUSTOM_USERCLUSTERFAIR": "tab:green",
    "CUSTOM_USERCLUSTERFAIR_PARTITIONER": "tab:red",
    "CUSTOM_RANDOM": "tab:purple",
    "CUSTOM_SHORT": "tab:brown",
    "CUSTOM_FAIR": "tab:cyan",
}

SCHEDULER_LINE = {
    "DEFAULT_FAIR": "-",
    "DEFAULT_FAIR_PARTITIONER": (0, (3, 1, 1, 1)),
    "CUSTOM_USERCLUSTERFAIR": (0, (5, 5)),
    "CUSTOM_USERCLUSTERFAIR_PARTITIONER": (0, (5, 1)),
    "CUSTOM_RANDOM": (5, (10, 3)),
    "CUSTOM_SHORT": (0, (1, 5)),
    "CUSTOM_FAIR": (0, (1, 1)),
}

CONFIGS = [
    "2_large_2_small_users",
    "4_large_users",
    "2_power_2_small_users",
    "4_super_small_users",
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


