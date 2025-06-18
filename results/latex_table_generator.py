import csv
import os
from globals import *





def maybe_bold(val, col_name, highlight_values, is_int=False):
    formatted = f"{int(round(val))}" if is_int else f"{val:.2f}"
    if val == highlight_values[col_name]:
        return f"\\textbf{{{formatted[:4]}}}"
    return formatted[:4]


def latex_2_large_2_small_users(df_avg):
    print("Making latex table for 2_large_2_small_users")


    cols_min = [
        'average response time',
        'average proportional slowdown',
        'average response time (worst10%)',
        'average proportional slowdown (worst10%)',
        'loop100_ average response time',
        'loop100_ average proportional slowdown',
        'loop1000_ average response time',
        'loop1000_ average proportional slowdown',
        'Total DVR',
        'Total violations',
        'Total DSR',
        'Total gains',
    ]


    cols_max = [
        'Total DSR',
        'Total gains',
    ]
    def format_row(row, highlight_max, highlight_min):
        if "UJF" in FORMAL_NAME[row['Scheduler']]:
            return (
            f"{FORMAL_NAME[row['Scheduler']]} & "
            f"{maybe_bold(row['average response time'], 'average response time', highlight_min)} | {maybe_bold(row['average proportional slowdown'], 'average proportional slowdown', highlight_min)} & "
            f"{maybe_bold(row['average response time (worst10%)'], 'average response time (worst10%)', highlight_min)} | {maybe_bold(row['average proportional slowdown (worst10%)'], 'average proportional slowdown (worst10%)', highlight_min)} & "
            f"{maybe_bold(row['loop100_ average response time'], 'loop100_ average response time', highlight_min)} | {maybe_bold(row['loop100_ average proportional slowdown'], 'loop100_ average proportional slowdown', highlight_min)} & "
            f"{maybe_bold(row['loop1000_ average response time'], 'loop1000_ average response time', highlight_min)} | {maybe_bold(row['loop1000_ average proportional slowdown'], 'loop1000_ average proportional slowdown', highlight_min)} & "
                f"- & - & "
                f"- & - \\\\")
        if "UWFQ" in FORMAL_NAME[row['Scheduler']]:

            return (
            f"{FORMAL_NAME[row['Scheduler']]} (this work) & "
            f"{maybe_bold(row['average response time'], 'average response time', highlight_min)} | {maybe_bold(row['average proportional slowdown'], 'average proportional slowdown', highlight_min)} & "
            f"{maybe_bold(row['average response time (worst10%)'], 'average response time (worst10%)', highlight_min)} | {maybe_bold(row['average proportional slowdown (worst10%)'], 'average proportional slowdown (worst10%)', highlight_min)} & "
            f"{maybe_bold(row['loop100_ average response time'], 'loop100_ average response time', highlight_min)} | {maybe_bold(row['loop100_ average proportional slowdown'], 'loop100_ average proportional slowdown', highlight_min)} & "
            f"{maybe_bold(row['loop1000_ average response time'], 'loop1000_ average response time', highlight_min)} | {maybe_bold(row['loop1000_ average proportional slowdown'], 'loop1000_ average proportional slowdown', highlight_min)} & "
                f"{maybe_bold(row['Total DVR'], 'Total DVR', highlight_min)} & {maybe_bold(row['Total violations'], 'Total violations', highlight_min, is_int=True)} & "
                f"{maybe_bold(row['Total DSR'], 'Total DSR', highlight_max)} & {maybe_bold(row['Total gains'], 'Total gains', highlight_max, is_int=True)} \\\\ \\hline"
            )
        return (
            f"{FORMAL_NAME[row['Scheduler']]} & "
            f"{maybe_bold(row['average response time'], 'average response time', highlight_min)} | {maybe_bold(row['average proportional slowdown'], 'average proportional slowdown', highlight_min)} & "
            f"{maybe_bold(row['average response time (worst10%)'], 'average response time (worst10%)', highlight_min)} | {maybe_bold(row['average proportional slowdown (worst10%)'], 'average proportional slowdown (worst10%)', highlight_min)} & "
            f"{maybe_bold(row['loop100_ average response time'], 'loop100_ average response time', highlight_min)} | {maybe_bold(row['loop100_ average proportional slowdown'], 'loop100_ average proportional slowdown', highlight_min)} & "
            f"{maybe_bold(row['loop1000_ average response time'], 'loop1000_ average response time', highlight_min)} | {maybe_bold(row['loop1000_ average proportional slowdown'], 'loop1000_ average proportional slowdown', highlight_min)} & "
                f"{maybe_bold(row['Total DVR'], 'Total DVR', highlight_min)} & {maybe_bold(row['Total violations'], 'Total violations', highlight_min, is_int=True)} & "
                f"{maybe_bold(row['Total DSR'], 'Total DSR', highlight_max)} & {maybe_bold(row['Total gains'], 'Total gains', highlight_max, is_int=True)} \\\\"
        )

    df_avg = df_avg.round(2)
    highlight_max = df_avg[cols_max].max()
    highlight_min = df_avg[cols_min].min()
    rows = [format_row(row, highlight_max, highlight_min) for _, row in df_avg.iterrows()]


    latex_table = r"""
\begin{table}[h!]
\centering
\resizebox{\textwidth}{!}{%
\begin{tabular}{|c|cccc|cc|cc|}
\hline
\textbf{Scheduler} & \multicolumn{4}{c|}{\textbf{Response time (s) | \textbf{Slowdown}}} & \multicolumn{4}{c|}{\textbf{Fairness}} \\
\cline{2-9}
& \textbf{Avg.} & \textbf{Worst 10\%} & \textbf{Short} & \textbf{Long} 
& \textbf{DVR} & \textbf{Count} & \textbf{DSR} & \textbf{Count}
\\
\hline




""" + "\n".join(rows) + r"""


\end{tabular}
}
\caption{Comparison of scheduler performance metrics for scenario 1.}
\label{table: micro-benchmark scenario 1}
\end{table}

"""

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_file = os.path.join(OUTPUT_DIR, f"scenario-1.tex")

    with open(output_file, "w") as f:
        f.write(latex_table)











###############################################################

def latex_2_power_2_small_users(df_avg):
    print("Making latex table for 2_power_2_small")
    
    cols_min = [
        'average response time',
        'average proportional slowdown',
        'average response time (worst10%)',
        'average proportional slowdown (worst10%)',
        'power_avg',
        'power_avg_slow',
        'small_avg',
        'small_avg_slow',
        'Total DVR',
        'Total violations',
        'Total DSR',
        'Total gains',
    ]


    cols_max = [
        'Total DSR',
        'Total gains',
    ]

    def format_row(row, highlight_max, highlight_min):
        if "UJF" in FORMAL_NAME[row['Scheduler']]:
            return (
                f"{FORMAL_NAME[row['Scheduler']]} & "
                f"{maybe_bold(row['average response time'], 'average response time', highlight_min)} | {maybe_bold(row['average proportional slowdown'], 'average proportional slowdown', highlight_min)} & "
                f"{maybe_bold(row['average response time (worst10%)'], 'average response time (worst10%)', highlight_min)} | {maybe_bold(row['average proportional slowdown (worst10%)'], 'average proportional slowdown (worst10%)', highlight_min)} & "
                f"{maybe_bold(row['power_avg'], 'power_avg', highlight_min)} | {maybe_bold(row['power_avg_slow'], 'power_avg_slow', highlight_min)} & "
                f"{maybe_bold(row['small_avg'], 'small_avg', highlight_min)} | {maybe_bold(row['small_avg_slow'], 'small_avg_slow', highlight_min)} & "
                f"- & - & "
                f"- & - \\\\")
        if "UWFQ" in FORMAL_NAME[row['Scheduler']]:

            return (

            f"{FORMAL_NAME[row['Scheduler']]} (this work) & "
            f"{maybe_bold(row['average response time'], 'average response time', highlight_min)} | {maybe_bold(row['average proportional slowdown'], 'average proportional slowdown', highlight_min)} & "
            f"{maybe_bold(row['average response time (worst10%)'], 'average response time (worst10%)', highlight_min)} | {maybe_bold(row['average proportional slowdown (worst10%)'], 'average proportional slowdown (worst10%)', highlight_min)} & "
            f"{maybe_bold(row['power_avg'], 'power_avg', highlight_min)} | {maybe_bold(row['power_avg_slow'], 'power_avg_slow', highlight_min)} & "
            f"{maybe_bold(row['small_avg'], 'small_avg', highlight_min)} | {maybe_bold(row['small_avg_slow'], 'small_avg_slow', highlight_min)} & "
            f"{maybe_bold(row['Total DVR'], 'Total DVR', highlight_min)} & {maybe_bold(row['Total violations'], 'Total violations', highlight_min, is_int=True)} & "
            f"{maybe_bold(row['Total DSR'], 'Total DSR', highlight_max)} & {maybe_bold(row['Total gains'], 'Total gains', highlight_max, is_int=True)} \\\\ \\hline"
            )
        return (
            f"{FORMAL_NAME[row['Scheduler']]} & "
            f"{maybe_bold(row['average response time'], 'average response time', highlight_min)} | {maybe_bold(row['average proportional slowdown'], 'average proportional slowdown', highlight_min)} & "
            f"{maybe_bold(row['average response time (worst10%)'], 'average response time (worst10%)', highlight_min)} | {maybe_bold(row['average proportional slowdown (worst10%)'], 'average proportional slowdown (worst10%)', highlight_min)} & "
            f"{maybe_bold(row['power_avg'], 'power_avg', highlight_min)} | {maybe_bold(row['power_avg_slow'], 'power_avg_slow', highlight_min)} & "
            f"{maybe_bold(row['small_avg'], 'small_avg', highlight_min)} | {maybe_bold(row['small_avg_slow'], 'small_avg_slow', highlight_min)} & "
                f"{maybe_bold(row['Total DVR'], 'Total DVR', highlight_min)} & {maybe_bold(row['Total violations'], 'Total violations', highlight_min, is_int=True)} & "
                f"{maybe_bold(row['Total DSR'], 'Total DSR', highlight_max)} & {maybe_bold(row['Total gains'], 'Total gains', highlight_max, is_int=True)} \\\\"
        )

    df_avg = df_avg[~(df_avg['Scheduler'].str.contains('PARTITIONER'))]
    df_avg['power_avg'] = (df_avg['user1_power_ average response time'] + df_avg['user2_power_ average response time']) / 2
    df_avg['power_avg_slow'] = (df_avg['user1_power_ average proportional slowdown'] + df_avg['user2_power_ average proportional slowdown']) / 2
    df_avg['small_avg'] = (df_avg['user3_small_ average response time'] + df_avg['user4_small_ average response time']) / 2
    df_avg['small_avg_slow'] = (df_avg['user3_small_ average proportional slowdown'] + df_avg['user4_small_ average proportional slowdown']) / 2
    df_avg = df_avg.round(2)
    highlight_max = df_avg[cols_max].max()
    highlight_min = df_avg[cols_min].min()
    rows = [format_row(row, highlight_max, highlight_min) for _, row in df_avg.iterrows()]


    latex_table = r"""
\begin{table}[h!]
\centering
\resizebox{\textwidth}{!}{%
\begin{tabular}{|c|cccc|cccc|cc|}
\hline
\textbf{Scheduler} & \multicolumn{4}{c|}{\textbf{Response time (s) | \textbf{Slowdown}}} & \multicolumn{4}{c|}{\textbf{Fairness}} \\
\cline{2-9}
& \textbf{Avg.} & \textbf{Worst 10\%} & \textbf{Freq.} & \textbf{Infreq.} 
& \textbf{DVR} & \textbf{Count} & \textbf{DSR} & \textbf{Count}
\\
\hline


""" + "\n".join(rows) + r"""

\end{tabular}
}
\caption{Comparison of scheduler performance metrics for scenario 2.}
\label{table: micro-benchmark scenario 2}
\end{table}

"""

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_file = os.path.join(OUTPUT_DIR, f"scenario-2.tex")

    with open(output_file, "w") as f:
        f.write(latex_table)








##################################################################################33




def latex_4_large_users(df_avg):
    print("Making latex table for 4_large_users")

    cols_min = [
        'average response time',
        'average proportional slowdown',
        'average response time (worst10%)',
        'average proportional slowdown (worst10%)',
        'best_avg',
        'best_avg_slow',
        'worst_avg',
        'worst_avg_slow',
        'Total DVR',
        'Total violations',
        'Total DSR',
        'Total gains',
    ]


    cols_max = [
        'Total DSR',
        'Total gains',
    ]

    def format_row(row, highlight_max, highlight_min):
        if "UJF" in FORMAL_NAME[row['Scheduler']]:
            return (
                f"{FORMAL_NAME[row['Scheduler']]} & "
                f"{maybe_bold(row['average response time'], 'average response time', highlight_min)} | {maybe_bold(row['average proportional slowdown'], 'average proportional slowdown', highlight_min)} & "
                f"{maybe_bold(row['average response time (worst10%)'], 'average response time (worst10%)', highlight_min)} | {maybe_bold(row['average proportional slowdown (worst10%)'], 'average proportional slowdown (worst10%)', highlight_min)} & "
                f"{maybe_bold(row['best_avg'], 'best_avg', highlight_min)} | {maybe_bold(row['best_avg_slow'], 'best_avg_slow', highlight_min)} & "
                f"{maybe_bold(row['worst_avg'], 'worst_avg', highlight_min)} | {maybe_bold(row['worst_avg_slow'], 'worst_avg_slow', highlight_min)} & "
                f"- & - & "
                f"- & - \\\\")
        if "UWFQ" in FORMAL_NAME[row['Scheduler']]:

            return (

            f"{FORMAL_NAME[row['Scheduler']]} (this work) & "
            f"{maybe_bold(row['average response time'], 'average response time', highlight_min)} | {maybe_bold(row['average proportional slowdown'], 'average proportional slowdown', highlight_min)} & "
            f"{maybe_bold(row['average response time (worst10%)'], 'average response time (worst10%)', highlight_min)} | {maybe_bold(row['average proportional slowdown (worst10%)'], 'average proportional slowdown (worst10%)', highlight_min)} & "
            f"{maybe_bold(row['best_avg'], 'best_avg', highlight_min)} | {maybe_bold(row['best_avg_slow'], 'best_avg_slow', highlight_min)} & "
            f"{maybe_bold(row['worst_avg'], 'worst_avg', highlight_min)} | {maybe_bold(row['worst_avg_slow'], 'worst_avg_slow', highlight_min)} & "
            f"{maybe_bold(row['Total DVR'], 'Total DVR', highlight_min)} & {maybe_bold(row['Total violations'], 'Total violations', highlight_min, is_int=True)} & "
            f"{maybe_bold(row['Total DSR'], 'Total DSR', highlight_max)} & {maybe_bold(row['Total gains'], 'Total gains', highlight_max, is_int=True)} \\\\ \\hline"
            )
        return (
            f"{FORMAL_NAME[row['Scheduler']]} & "
            f"{maybe_bold(row['average response time'], 'average response time', highlight_min)} | {maybe_bold(row['average proportional slowdown'], 'average proportional slowdown', highlight_min)} & "
            f"{maybe_bold(row['average response time (worst10%)'], 'average response time (worst10%)', highlight_min)} | {maybe_bold(row['average proportional slowdown (worst10%)'], 'average proportional slowdown (worst10%)', highlight_min)} & "
            f"{maybe_bold(row['best_avg'], 'best_avg', highlight_min)} | {maybe_bold(row['best_avg_slow'], 'best_avg_slow', highlight_min)} & "
            f"{maybe_bold(row['worst_avg'], 'worst_avg', highlight_min)} | {maybe_bold(row['worst_avg_slow'], 'worst_avg_slow', highlight_min)} & "
                f"{maybe_bold(row['Total DVR'], 'Total DVR', highlight_min)} & {maybe_bold(row['Total violations'], 'Total violations', highlight_min, is_int=True)} & "
                f"{maybe_bold(row['Total DSR'], 'Total DSR', highlight_max)} & {maybe_bold(row['Total gains'], 'Total gains', highlight_max, is_int=True)} \\\\"
        )


    response_cols = [
        'user1_big_ average response time',
        'user2_big_ average response time',
        'user3_big_ average response time',
        'user4_big_ average response time',
    ]

    slowdown_cols = [
        'user1_big_ average proportional slowdown',
        'user2_big_ average proportional slowdown',
        'user3_big_ average proportional slowdown',
        'user4_big_ average proportional slowdown',
    ]

# Compute row-wise min and max for response time and slowdown
    df_avg['best_avg'] = df_avg[response_cols].min(axis=1)
    df_avg['worst_avg'] = df_avg[response_cols].max(axis=1)

    df_avg['best_avg_slow'] = df_avg[slowdown_cols].min(axis=1)
    df_avg['worst_avg_slow'] = df_avg[slowdown_cols].max(axis=1)

    df_avg = df_avg.round(2)
    highlight_max = df_avg[cols_max].max()
    highlight_min = df_avg[cols_min].min()
    rows = [format_row(row, highlight_max, highlight_min) for _, row in df_avg.iterrows()]


    latex_table = r"""
\begin{table}[h!]
\centering
\resizebox{\textwidth}{!}{%
\begin{tabular}{|c|cccc|cccc|cc|}
\hline
\textbf{Scheduler} & \multicolumn{4}{c|}{\textbf{Response time (s) | \textbf{Slowdown}}} & \multicolumn{4}{c|}{\textbf{Fairness}} \\
\cline{2-9}
& \textbf{Avg.} & \textbf{Worst 10\%} & \textbf{Worst} & \textbf{Best} 
& \textbf{DVR} & \textbf{Count} & \textbf{DSR} & \textbf{Count}
\\
\hline


""" + "\n".join(rows) + r"""


\end{tabular}
}
\caption{Comparison of scheduler performance metrics for scenario 3.}
\label{table: micro-benchmark scenario 3}
\end{table}

"""

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_file = os.path.join(OUTPUT_DIR, f"scenario-3.tex")

    with open(output_file, "w") as f:
        f.write(latex_table)






####################################################################################################################




def latex_4_super_small_users(df_avg):
    print("Making latex table for 4_super_small")



    cols_min = [
        'average response time',
        'average proportional slowdown',
        'average response time (worst10%)',
        'average proportional slowdown (worst10%)',
        'best_avg',
        'best_avg_slow',
        'worst_avg',
        'worst_avg_slow',
        'Total DVR',
        'Total violations',
        'Total DSR',
        'Total gains',
    ]


    cols_max = [
        'Total DSR',
        'Total gains',
    ]

    def format_row(row, highlight_max, highlight_min):
        if "UJF" in FORMAL_NAME[row['Scheduler']]:
            return (
                f"{FORMAL_NAME[row['Scheduler']]} & "
                f"{maybe_bold(row['average response time'], 'average response time', highlight_min)} | {maybe_bold(row['average proportional slowdown'], 'average proportional slowdown', highlight_min)} & "
                f"{maybe_bold(row['average response time (worst10%)'], 'average response time (worst10%)', highlight_min)} | {maybe_bold(row['average proportional slowdown (worst10%)'], 'average proportional slowdown (worst10%)', highlight_min)} & "
                f"{maybe_bold(row['best_avg'], 'best_avg', highlight_min)} | {maybe_bold(row['best_avg_slow'], 'best_avg_slow', highlight_min)} & "
                f"{maybe_bold(row['worst_avg'], 'worst_avg', highlight_min)} | {maybe_bold(row['worst_avg_slow'], 'worst_avg_slow', highlight_min)} & "
                f"- & - & "
                f"- & - \\\\")
        if "UWFQ" in FORMAL_NAME[row['Scheduler']]:

            return (

            f"{FORMAL_NAME[row['Scheduler']]} (this work) & "
            f"{maybe_bold(row['average response time'], 'average response time', highlight_min)} | {maybe_bold(row['average proportional slowdown'], 'average proportional slowdown', highlight_min)} & "
            f"{maybe_bold(row['average response time (worst10%)'], 'average response time (worst10%)', highlight_min)} | {maybe_bold(row['average proportional slowdown (worst10%)'], 'average proportional slowdown (worst10%)', highlight_min)} & "
            f"{maybe_bold(row['best_avg'], 'best_avg', highlight_min)} | {maybe_bold(row['best_avg_slow'], 'best_avg_slow', highlight_min)} & "
            f"{maybe_bold(row['worst_avg'], 'worst_avg', highlight_min)} | {maybe_bold(row['worst_avg_slow'], 'worst_avg_slow', highlight_min)} & "
            f"{maybe_bold(row['Total DVR'], 'Total DVR', highlight_min)} & {maybe_bold(row['Total violations'], 'Total violations', highlight_min, is_int=True)} & "
            f"{maybe_bold(row['Total DSR'], 'Total DSR', highlight_max)} & {maybe_bold(row['Total gains'], 'Total gains', highlight_max, is_int=True)} \\\\ \\hline"
            )
        return (
            f"{FORMAL_NAME[row['Scheduler']]} & "
            f"{maybe_bold(row['average response time'], 'average response time', highlight_min)} | {maybe_bold(row['average proportional slowdown'], 'average proportional slowdown', highlight_min)} & "
            f"{maybe_bold(row['average response time (worst10%)'], 'average response time (worst10%)', highlight_min)} | {maybe_bold(row['average proportional slowdown (worst10%)'], 'average proportional slowdown (worst10%)', highlight_min)} & "
            f"{maybe_bold(row['best_avg'], 'best_avg', highlight_min)} | {maybe_bold(row['best_avg_slow'], 'best_avg_slow', highlight_min)} & "
            f"{maybe_bold(row['worst_avg'], 'worst_avg', highlight_min)} | {maybe_bold(row['worst_avg_slow'], 'worst_avg_slow', highlight_min)} & "
                f"{maybe_bold(row['Total DVR'], 'Total DVR', highlight_min)} & {maybe_bold(row['Total violations'], 'Total violations', highlight_min, is_int=True)} & "
                f"{maybe_bold(row['Total DSR'], 'Total DSR', highlight_max)} & {maybe_bold(row['Total gains'], 'Total gains', highlight_max, is_int=True)} \\\\"
        )

    df_avg = df_avg[~(df_avg['Scheduler'].str.contains('PARTITIONER'))]



    response_cols = [
        'user1_supersmall_ average response time',
        'user2_supersmall_ average response time',
        'user3_supersmall_ average response time',
        'user4_supersmall_ average response time',
    ]

    slowdown_cols = [
        'user1_supersmall_ average proportional slowdown',
        'user2_supersmall_ average proportional slowdown',
        'user3_supersmall_ average proportional slowdown',
        'user4_supersmall_ average proportional slowdown',
    ]

# Compute row-wise min and max for response time and slowdown
    df_avg['best_avg'] = df_avg[response_cols].min(axis=1)
    df_avg['worst_avg'] = df_avg[response_cols].max(axis=1)

    df_avg['best_avg_slow'] = df_avg[slowdown_cols].min(axis=1)
    df_avg['worst_avg_slow'] = df_avg[slowdown_cols].max(axis=1)



    df_avg = df_avg.round(2)
    highlight_max = df_avg[cols_max].max()
    highlight_min = df_avg[cols_min].min()
    rows = [format_row(row, highlight_max, highlight_min) for _, row in df_avg.iterrows()]


    latex_table = r"""
\begin{table}[h!]
\centering
\resizebox{\textwidth}{!}{%
\begin{tabular}{|c|cccc|cccc|cc|}
\hline
\textbf{Scheduler} & \multicolumn{4}{c|}{\textbf{Response time (s) | \textbf{Slowdown}}} & \multicolumn{4}{c|}{\textbf{Fairness}} \\
\cline{2-9}
& \textbf{Avg.} & \textbf{Worst 10\%} & \textbf{First} & \textbf{Last} 
& \textbf{DVR} & \textbf{Count} & \textbf{DSR} & \textbf{Count}
\\
\hline


""" + "\n".join(rows) + r"""

\end{tabular}
}
\caption{Comparison of scheduler performance metrics for scenario 4.}
\label{table: micro-benchmark scenario 4}
\end{table}



"""

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_file = os.path.join(OUTPUT_DIR, f"scenario-4.tex")

    with open(output_file, "w") as f:
        f.write(latex_table)













def latex_hetero_macro(df_avg):
    print("Making latex table for hetero_macro")
def latex_homo_macro(df_avg):
    print("Making latex table for homo_macro")



