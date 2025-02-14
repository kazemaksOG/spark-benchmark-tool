# Spark benchmarking tool
This is a tool used for testing the Spark framework itself rather than the underlying system. For this particular project, it is meant to test and see how different schedulers affect throughput and fairness of the application.


## Structure
* Main - used for testing around. Can be submitted the same way BenchRunner, just replace BenchRunner class with Main when launching.



## Results
Results can be obtained by running the script in `results/visualize__results.py`. It relies on the output results from the benchmarks and the history server running on localhost to get even data.



