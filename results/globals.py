

# result parsing settings

EXECUTOR_AMOUNT = 8
CORES_PER_EXEC = 4
CORES = EXECUTOR_AMOUNT * CORES_PER_EXEC

# Macro benchmark settings
TIME_FRAME_S = 500
SCALING = 10
MACRO_MAX_JOB_RUNTIME_S = 60

PARALLELIZATION_SCALING = 24

FIG_FORMAT = "svg"

RUN_PATH="./data/microbench_7/target"
BENCH_PATH=f"{RUN_PATH}/bench_outputs"
OUTPUT_DIR="./metrics"

# history server address
APPS_URL="http://localhost:18080/api/v1/applications"


SCHEDULERS = [
    "CUSTOM_FAIR_PARTITIONER",
    "CUSTOM_FAIR",

    "CUSTOM_RANDOM_PARTITIONER",
    "CUSTOM_RANDOM",

    "TRUE_FIFO_PARTITIONER",
    "TRUE_FIFO",

    "DEFAULT_FAIR_PARTITIONER",
    "DEFAULT_FAIR",

    "CUSTOM_CLUSTERFAIR_PARTITIONER",
    "CUSTOM_CLUSTERFAIR",

    "CUSTOM_USERCLUSTERFAIR_PARTITIONER", # has to be before regular because of string comparison
    "CUSTOM_USERCLUSTERFAIR",

    # "CUSTOM_SHORT",
    # "AQE_CUSTOM_FAIR",
    # "AQE_CUSTOM_RANDOM",
    # "AQE_CUSTOM_SHORT",
    # "AQE_DEFAULT_FIFO",
    # "AQE_DEFAULT_FAIR",
]

FORMAL_NAME = {
    "CUSTOM_FAIR_PARTITIONER": "UJF-P",
    "CUSTOM_FAIR": "UJF",

    "CUSTOM_RANDOM_PARTITIONER": "Random-P",
    "CUSTOM_RANDOM": "Random",

    "TRUE_FIFO_PARTITIONER": "T-Fifo-P",
    "TRUE_FIFO": "T-Fifo",

    "DEFAULT_FAIR_PARTITIONER": "Fair-P",
    "DEFAULT_FAIR": "Fair",

    "CUSTOM_CLUSTERFAIR_PARTITIONER": "CFQ-P",
    "CUSTOM_CLUSTERFAIR": "CFQ",

    "CUSTOM_USERCLUSTERFAIR_PARTITIONER": "UWFQ-P", # has to be before regular because of string comparison
    "CUSTOM_USERCLUSTERFAIR": "UWFQ",
}


SCHEDULER_COLOR = {


    "CUSTOM_FAIR_PARTITIONER": "tab:blue",
    "CUSTOM_FAIR": "tab:blue",

    "CUSTOM_RANDOM_PARTITIONER": "tab:orange",
    "CUSTOM_RANDOM": "tab:orange",

    "TRUE_FIFO_PARTITIONER": "tab:green",
    "TRUE_FIFO": "tab:green",

    "DEFAULT_FAIR_PARTITIONER": "tab:red",
    "DEFAULT_FAIR": "tab:red",

    "CUSTOM_CLUSTERFAIR_PARTITIONER": "tab:purple",
    "CUSTOM_CLUSTERFAIR": "tab:purple",

    "CUSTOM_USERCLUSTERFAIR_PARTITIONER": "tab:brown", # has to be before regular because of string comparison
    "CUSTOM_USERCLUSTERFAIR": "tab:brown",

}

SCHEDULER_LINE = {
    "CUSTOM_FAIR_PARTITIONER": "-",
    "CUSTOM_FAIR": "-",

    "CUSTOM_RANDOM_PARTITIONER": (5, (10, 3)),
    "CUSTOM_RANDOM": (5, (10, 3)),

    "TRUE_FIFO_PARTITIONER": (0, (1, 5)),
    "TRUE_FIFO": (0, (1, 5)),

    "DEFAULT_FAIR_PARTITIONER":  (0, (5, 5)),
    "DEFAULT_FAIR":  (0, (5, 5)),

    "CUSTOM_CLUSTERFAIR_PARTITIONER": (0, (3, 1, 1, 1)),
    "CUSTOM_CLUSTERFAIR": (0, (3, 1, 1, 1)),

    "CUSTOM_USERCLUSTERFAIR_PARTITIONER": (0, (5, 1)), # has to be before regular because of string comparison
    "CUSTOM_USERCLUSTERFAIR": (0, (5, 1)),



}

CONFIGS = [
    "2_large_2_small_users",
    "4_large_users",
    "2_power_2_small_users",
    "4_super_small_users",
    "macro_config",
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


