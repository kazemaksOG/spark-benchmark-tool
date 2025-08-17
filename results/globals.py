

# result parsing settings

EXECUTOR_AMOUNT = 8
CORES_PER_EXEC = 4
CORES = EXECUTOR_AMOUNT * CORES_PER_EXEC
FIG_FORMAT = "svg"
TASK_TRACKING_ENABLED = False
PAPER_FIGSIZE = (8,5)
FONT_SIZE=14

# Macro benchmark settings

MACRO_CONFIG="./google_benchmark_500s/macro_benchmarks_hetero.csv"

# Scaling scales each job runtime by that factor
SCALING = 2 # For existing benchmarks: Hetero 2, homo 20
# Filter large filters jobs that are 5x bigger than the median
FILTER_LARGE = False # For existing benchmarks: Hetero False, homo True

TIME_FRAME_S = 500 # The length of the timeframe, for homo and hetero experiements it is 500
TIME_SCALE = 1 # The scaler for time, which scales job arrival times by that amount (untested)

# expected speedup of parallelizing the jobs, usually lower than CORES (70% of the cores in the system or even lower, original system had 32)
PARALLELIZATION_SCALING = 24


# paths to benchmarks

BENCHMARK_PATH="./data/homo_benchmark/"
FILTER_DEFAULT = True
BENCH_PATH=f"{BENCHMARK_PATH}/target/bench_outputs"
OUTPUT_DIR="./metrics"

# history server address
APPS_URL="http://localhost:18080/api/v1/applications"


SCHEDULER_ORDER = [

    "DEFAULT_FAIR",
    "DEFAULT_FAIR_PARTITIONER",

    "CUSTOM_FAIR",
    "CUSTOM_FAIR_PARTITIONER",

    "CUSTOM_RANDOM",
    "CUSTOM_RANDOM_PARTITIONER",

    "TRUE_FIFO",
    "TRUE_FIFO_PARTITIONER",


    "CUSTOM_CLUSTERFAIR",
    "CUSTOM_CLUSTERFAIR_PARTITIONER",

    "CUSTOM_USERCLUSTERFAIR",
    "CUSTOM_USERCLUSTERFAIR_PARTITIONER", # has to be before regular because of string comparison

    # "CUSTOM_SHORT",
    # "AQE_CUSTOM_FAIR",
    # "AQE_CUSTOM_RANDOM",
    # "AQE_CUSTOM_SHORT",
    # "AQE_DEFAULT_FIFO",
    # "AQE_DEFAULT_FAIR",
]


# SCHEDULER_ORDER = [
#
#     "DEFAULT_FAIR",
#
#     "CUSTOM_FAIR",
#
#     "CUSTOM_RANDOM",
#
#     "TRUE_FIFO",
#
#
#     "CUSTOM_CLUSTERFAIR",
#
#     "CUSTOM_USERCLUSTERFAIR",
#
#
#
#     "DEFAULT_FAIR_PARTITIONER",
#     "CUSTOM_FAIR_PARTITIONER",
#     "CUSTOM_RANDOM_PARTITIONER",
#     "TRUE_FIFO_PARTITIONER",
#     "CUSTOM_CLUSTERFAIR_PARTITIONER",
#     "CUSTOM_USERCLUSTERFAIR_PARTITIONER", # has to be before regular because of string comparison
#
#     # "CUSTOM_SHORT",
#     # "AQE_CUSTOM_FAIR",
#     # "AQE_CUSTOM_RANDOM",
#     # "AQE_CUSTOM_SHORT",
#     # "AQE_DEFAULT_FIFO",
#     # "AQE_DEFAULT_FAIR",
# ]

SCHEDULERS = [

    "DEFAULT_FAIR_PARTITIONER",
    "DEFAULT_FAIR",

    "CUSTOM_FAIR_PARTITIONER",
    "CUSTOM_FAIR",

    "CUSTOM_RANDOM_PARTITIONER",
    "CUSTOM_RANDOM",

    "TRUE_FIFO_PARTITIONER",
    "TRUE_FIFO",


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
    "hetero_macro",
    "homo_macro",
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


