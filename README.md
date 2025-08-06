# Spark benchmarking tool
This is a tool used for testing the Spark framework itself rather than the underlying system. For this particular project, it is meant to test and see how different schedulers affect response times, throughput and fairness of the application.


## Structure
* `configs` - configurations used for running experiments
* `estimators` - performance estimators that predict job runtime which is used by `schedulers` and `partitioners`
* `schedulers` - custom schedulers that can be class loaded into Apache Spark framework. 
* `partitioners` - custom partitioners that can be class loaded into Apache Spark framework. 
* `results` - collection of previous results and tools necessary for parsing Spark benchmarking tool outputs.
* `resources` - directory containing all necessary data resources needed to run the experiments.
* `src` - java source files that 
    - `BenchRunner` - Spark application that is launched to run the Spark benchmarking tool.
    - `Main` - used for testing different pieces of code. Can be submitted the same way BenchRunner, just replace BenchRunner class with Main when launching.
    - `GoogleTraceParser` - used for parsing Google traces into Spark applicable traces. Code has to be manually modified and recompiled to adjust the trace timeframe. Can be submitted the same way BenchRunner, just replace BenchRunner class with GoogleTraceParser when launching.
    - `RepartitionTaxiData` - used for repartitioning TLC dataset parquet file to multiple files. Can be submitted the same way BenchRunner, just replace BenchRunner class with RepartitionTaxiData when launching.

## DAS5 login and setup
DAS5 is hosted on multiple universities, but some endpoints are not maintained. We use `fs0.das5.cs.vu.nl` for these experiments. 

To access the node as a TU Student or employee, it is first necessary to ssh into the TU Delft bastion. If not in universtiy premises, edurom VPN must be setup, see [https://www.eduvpn.org/client-apps](https://www.eduvpn.org/client-apps/).

```ssh username@linux-bastion.tudelft.nl```

From there, the node can be accessed


```ssh username@fs0.das5.cs.vu.nl```


**It is highly advised** to perform all work in `/var/scratch/$USER` rather than home directory `~`, since scratch has much bigger storage.

## Setup
Scripts have been tested on DAS5 and require `bash` to be the shell sourcing them. These scripts depend on the following components to be present.

* **DAS5 big data deployment framework** - To deploy Spark on DAS5, "Deployment scripts for big data frameworks on DAS-5" framework was used and lightly extended. This can be be found here: [https://github.com/kazemaksOG/das-bigdata-deployment-python3](https://github.com/kazemaksOG/das-bigdata-deployment-python3)

* **Custom schedulers** - Custom schedulers are under the `schedulers` directory. They have to be compiled and added as arguments to Spark launcher.

* **Custom spark** - A modified Spark version is used to run these tests. Modifications enable custom scheduler and partitioner class loading. This can be found in here: [https://github.com/kazemaksOG/spark-3.5.5-custom#](https://github.com/kazemaksOG/spark-3.5.5-custom#)
* **Java version** - All maven modules have been tested with `java-17-openjdk`. Other java versions could work, but present no guarantees.

* **Data resources** - Resources needed to perform the experiments. For micro-benchmarks, we use High Volume For-Hire Vehicle Trip Records from [TLC](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (August 2024). And for macro-benchmarks, we use [Google traces](https://zenodo.org/records/3254540) standardized by WTA.


**Note: recommended to launch `screen` before starting the setup, since commands are not killed in screen sessions, and can reattach back to sessions in case of a disconnect.**

To setup the environment for the scripts:
1. Clone the DAS5 deployment framework and this repository.
```
git clone https://github.com/kazemaksOG/das-bigdata-deployment-python3.git
git clone https://github.com/kazemaksOG/spark-benchmark-tool.git
```

2. Modify the scripts to point to the correct directories.
```
vim ./spark-benchmark-tool/master-env.sh # SPARK_HOME
vim ./spark-benchmark-tool/setup_cluster.sh # DEPLOYER_HOME
vim ./spark-benchmark-tool/run_all_benchmarks.sh # PROJECT_DIR
```

3. Clone the Custom spark inside deployment framework.
```
mkdir das-bigdata-deployment-python3/frameworks
cd das-bigdata-deployment-python3/frameworks
git clone https://github.com/kazemaksOG/spark-3.5.5-custom.git
```

4. Compile custom spark with either `sbt` or `mvn` provided in `build`. **Note:** `sbt` is much faster to compile, but I found that sometimes there are some dependencies issues that magically are resolved by first compiling using `mvn`. This may take multiple hours
```
source ../../spark-benchmark-tool/master-env.sh # needed to load correct java module
cd spark-3.5.5-custom
./build/mvn -DskipTests package 
# or ./build/sbt package
```
**Optional**: If using a different spark version, modify das deployer `big_data_deployer/spark.py` to have a line on the bottom that adds the coresponing spark version. Template dir are settings used in `conf/spark/<TEMPLATE DIR>`. 

```python
get_framework_registry().framework("spark").add_version(SparkFrameworkVersion(<VERSION>, <GIT LINK OR ARCHIVE>, <GIT OR TGZ>, <NAME_OF_ROOT_DIR>, <TEMPLATE DIR>)
```

5. Add jar file to the maven repository
```
export SPARK_HOME=$(pwd)
./build/mvn install:install-file -Dfile=$SPARK_HOME/core/target/spark-core_2.12-3.5.5.jar -DgroupId=org.apache.spark -DartifactId=spark-core_2.12 -Dversion=3.5.5-custom -Dpackaging=jar
./build/mvn install:install-file -Dfile=$SPARK_HOME/sql/core/target/spark-sql_2.12-3.5.5.jar -DgroupId=org.apache.spark -DartifactId=spark-sql_2.12 -Dversion=3.5.5-custom -Dpackaging=jar
```

6. Compile the benchmarking tool and schedulers using `compile_mvn.sh`. May need to modify the mvn location at the top of the script.
```
cd ../../../spark-benchmark-tool # move to benchmark dir
source master-env.sh
bash compile_mvn.sh
```

7. Download the necessary resources for experiments.

```
cd spark-benchmark-tool/resources
curl -L -o taxi-data.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-08.parquet
```

8. Use Spark to repartition tripdata.

```
mkdir /tmp/spark-events # Sometimes it throws an error that it doesnt exist, so best to create it
chmod 777 /tmp/spark-events # It needs access from any user, since spark executors need to write to it
$SPARK_HOME/bin/spark-submit --class "RepartitionTaxiData" --master "local[2]" target/performance_test-1.0-SNAPSHOT.jar &> output.txt
```

**Note:** if using screen, `ctrl+a ctrl+c` can be used to crate a new window and view the `output.txt` file to track progress 

**Note:** This may fail if `setup_cluster.sh` was run before, since it destroys local settings for running Spark on headnode. In that case, either run this on a compute node, or replace the settings to be default.

**Optional:** Can also use `GoogleTraceParser` to parse google traces if different timeframe is needed for the macro-experiment. The source file must be modified and recompiled, then submitted. This will create a csv excerpt of the google trace, which then can be tranformed into a benchmark config file (see Running section).

```
curl -L -o google-dataset.zip https://zenodo.org/records/3254540/files/Google_parquet.zip\?download\=1
unzip google-dataset.zip
bash compile_mvn.sh 
$SPARK_HOME/bin/spark-submit --class "GoogleTraceParser" --master "local[2]" target/performance_test-1.0-SNAPSHOT.jar &> output.txt
```


## Running

**Note: Don't forget to recompile any file that was changed.**

**Note: recommended to launch `screen` before starting the setup, since commands are not killed in screen sessions, and can reattach back to sessions in case of a disconnect.**

1. Modify the `Deployer variables` on top of `setup_cluster.sh`, and all relevant variables in `run_all_benchmarks.sh` (iteration amount, WORKLOAD_DIR (workload is the benchmark, synthetic is micro-benchmarks, homo_macro is the homogeneious macro-benchmark), comment in/out schedulers that will be run) . Then source it. Can optionally supply the reservation ID if already made.
```
source setup_cluster.sh true <OPTIONAL_RESERVATION_ID>
```

**Note:** if deployment fails due to python error (can be observed in logs created in `das-bigdata-deployment-python3` directory), then some python dependencies might be missing. This can be solved by creating a virtual environement `venv` in `das-bigdata-deployment-python3` directory, sourcing it, and installing `requirements.txt`.

2. Read printed output, the benchmark should run automatically

**Note:** if using screen, `ctrl+a ctrl+c` can be used to crate a new window and view the `output.txt` file to track progress 


### Google traces 

Google traces uses the same taxi dataset as input for calculation, but schedules jobs based on the extracted google trace. The heterogeneous and homogeneous macro benchmark configs are already provided for running experiments, but it can also be replaced. This can be done by:

1. Performing the optional step 8. to get the Google trace timeframe.

2. Making the trace into a config. This can be done using `macro_bench_analysis.py`. First adjust all the necessary paths and variables in `globals.py` for `Macro benchmark settings`, and adjust core amount and exectuor amount in `result parsing settings`.

3. Run `macro_bench_analysis.py`, which will generate a config file.

```
pip3 install -r requirements.txt
python3 macro_bench_analysis.py
```

4. move the conifg file into the `configs` folder, create a directory for that file and place it in it, and change `run_all_benchmarks.sh` to run this workload.
```
mkdir ../configs/macro_config
mv config.json ../configs/macro_config
```

5. Experiements can be run as usual


## Results
Results can be obtained by running the script in `results/visualize_results.py`. It relies on the output results from the benchmarks and the history server running on localhost to get even data. 


**Note: after parsing is done, data is saved in `DATADUMP.data`, which will be reused to save some resources, and also omit the need to turn on the history server. However, if there is a bug in the parsing, it must be deleted to reparse the data**
### DAS5 setup

1. Source the `master-env.sh`
```
source master-env.sh
```
2. Make python virtual environment and install all dependencies

```
cd results
python3 -m venv ./venv
source ./venv/bin/activate
python3 -m pip install --upgrade pip # update pip, needed for some dependencies
pip3 install --user --only-binary=:all: Pillow # Pillow is missing linker files, so have to install only binary
pip3 install matplotlib 
pip3 install -r requirements.txt # rest should work normally

```


3. Start the spark server 
```
bash run_server.sh
```
4. Change globals to wanted values. The `RUN_PATH` should point to benchmark output parent directory, by default:

```
RUN_PATH="../target"
...
APPS_URL="http://fs0.das5.cs.vu.nl:18080/api/v1/applications" # local host seems to be unmapped to headnode?
```

5. Perform necessary analysis.

```
python3 visualize_results.py --help # to get all possible commands
```

6. Once done, stop the server 

```
bash stop_server.sh
```

### Local setup

1. Gather the event logs (should be in /var/scratch/$USER/eventlogs) and benchmark outputs ($PROJECT_ROOT/target/becnh_outputs) and zip them into a file. `scp` data over to your local machine:
```
bash gather_and_clean_results.sh # might have to modify if result paths differ. it also automatically deletes all zipped files
scp -oProxyJump=<USER>@linux-bastion.tudelft.nl <USER>@fs0.das5.cs.vu.nl:/var/scratch/<USER>/performance_test/results_<DATE>.tar.gz .

```
2. Unzip this file into a local instance of `spark-benchmark-tool/results/data`.

**Note**: Some statistics and visuals depend on BASE runtimes to make calculations. These must be present for the script to work. These are enabled by setting `RUN_INDIVIDUAL=1` in `run_all_benchmarks.sh`.

2. Change the paths in `globals.py` to reflect that location (`BENCHMARK_PATH`).
3. Setup a local Spark History Server with the extracted event logs. To ensure that no jobs are left out from the analysis, launch it with:
```
SPARK_DAEMON_MEMORY=32g SPARK_DAEMON_JAVA_OPTS="-Dspark.ui.retainedJobs=100000 -Dspark.ui.retainedStages=100000 -Dspark.ui.retainedTasks=10000000" $SPARK_HOME/sbin/start-history-server.sh
```
5. launch the python script with `python3 visualize_results <COMMAND>`


### Paper visual and table scripts 
This section documents the commands to run to get figures for the tables used in the paper/thesis.

```
TODO



```

