import json
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
from datetime import datetime

benchmark_dir="target"



def draw_schedule(data, filename):
    fig, ax = plt.subplots(figsize=(10, 6))

    user_colors = {
        "user1": 'blue',
        "user2": 'brown',
        "user3": 'orange',
        "user4": 'purple',
    }
    y_position = 0
    first_job=None
    last_job=None
    for user_data in data['users']:
        user_name = user_data['user'].split()[0]
        color = user_colors.get(user_name, 'black')  # Default color if not specified

        # Iterate through the workloads of each user
        y_offset = y_position
        for workload in user_data['workloads']:
            # Extract the start and end times for each workload's results
            for key, result in workload['results'].items():
                start_time = result.get('start_time-execution_time')
                end_time = result.get('end_time-execution_time')

                if first_job == None or first_job > start_time:
                    first_job = start_time

               

                if end_time == None:
                    
                    start_time_dt = datetime.utcfromtimestamp(start_time / 1000)  # Convert from ms to seconds
                    end_time_dt = datetime.utcfromtimestamp(1 + start_time / 1000)
                    ax.scatter(end_time_dt, y_offset, color='black', s=100, zorder=2)  # End marker
                    continue

                if last_job == None or last_job < end_time:
                    last_job = end_time


                
                # Convert timestamps to datetime objects
                start_time_dt = datetime.utcfromtimestamp(start_time / 1000)  # Convert from ms to seconds
                end_time_dt = datetime.utcfromtimestamp(end_time / 1000)  # Convert from ms to seconds
                
                # Plot the job as a horizontal line on the timeline

                ax.plot([start_time_dt, end_time_dt], [y_offset, y_offset], color=color, linewidth=2)
                ax.scatter(start_time_dt, y_offset, color='green', s=100, zorder=2)  # Start marker
                ax.scatter(end_time_dt, y_offset, color='red', s=100, zorder=2)  # End marker
                y_offset+=0.3
            y_offset=y_position
            y_offset+=0.05
        y_position+=10


    total_time = (last_job - first_job) / 1000.0
# draw the plot
    ax.set_xlabel('Time')
    ax.set_ylabel('Events')
    ax.set_title(filename +': ' + str(total_time) + 's')

    ax.grid(True, which='both', axis='x', linestyle='--', color='gray', alpha=0.5)

    ax.xaxis.set_major_locator(mdates.SecondLocator(interval=5))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%M:%S'))
    plt.xticks(rotation=45)
    plt.tight_layout()

    # plt.show()
    plt.savefig(filename + "user_job_timeline.png")








for filename in os.listdir(benchmark_dir):

    if filename.endswith('.json'):
        # Construct the full path of the JSON file
        file_path = os.path.join(benchmark_dir, filename)
        
        # Open and load the JSON file
        with open(file_path, 'r') as file:
            data = json.load(file)
            draw_schedule(data, filename)




