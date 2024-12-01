import json
import os
from typing import Tuple
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

def create_local_context() -> Tuple[SparkContext, SparkSession]:
    local_conf = SparkConf().setAll([
        ("spark.app.name", 'local_spark_app'),
        ("spark.master", "local[*]"),
        # ("spark.driver.bindAddress", "0.0.0.0"),
        ("spark.sql.caseSensitive", False),
        ("spark.cores.max", 6),
        ("spark.executor.cores", 6),
        ("spark.executor.memory", "8g"),
        ("spark.driver.memory", "8g"),
        ("spark.setMaster", "local[*]")
    ])

    spark: SparkSession = SparkSession.builder.config(conf=local_conf).getOrCreate()

    sc: SparkContext = spark.sparkContext

    return sc, spark

def create_remote_context() -> Tuple[SparkContext, SparkSession]:
    # Two files are automatically read: JobParameters.json for the Spark Cluster job using a temporary spark instance
    # and JobParameters.json for the Jupyter Lab job to extract the hostname of the cluster. 
    MASTER_HOST_NAME = None

    # Open the parameters Jupyter Lab app was launched with
    with open('/work/JobParameters.json', 'r') as file:
        JUPYTER_LAB_JOB_PARAMS = json.load(file)
        # from pprint import pprint; pprint(JUPYTER_LAB_JOB_PARAMS) 
        for resource in JUPYTER_LAB_JOB_PARAMS['request']['resources']:
            if 'hostname' in resource.keys():
                MASTER_HOST_NAME = resource['hostname']

    MASTER_HOST = f"spark://{MASTER_HOST_NAME}:7077"

    conf = SparkConf().setAll([
            ("spark.app.name", 'reading_job_params_app'), 
            ("spark.master", MASTER_HOST),
        ])
    spark = SparkSession.builder.config(conf=conf)\
                                .getOrCreate()

    CLUSTER_PARAMETERS_JSON_DF = spark.read.option("multiline","true").json('/work/JobParameters.json')

    # Extract cluster info from the specific JobParameters.json
    NODES = CLUSTER_PARAMETERS_JSON_DF.select("request.replicas").first()[0]
    CPUS_PER_NODE = CLUSTER_PARAMETERS_JSON_DF.select("machineType.cpu").first()[0] - 1
    MEM_PER_NODE = CLUSTER_PARAMETERS_JSON_DF.select("machineType.memoryInGigs").first()[0]

    CLUSTER_CORES_MAX = CPUS_PER_NODE * NODES
    CLUSTER_MEMORY_MAX = MEM_PER_NODE * NODES 
    
    if CPUS_PER_NODE > 1:
        EXECUTOR_CORES = CPUS_PER_NODE - 1  # set cores per executor on worker node
    else:
        EXECUTOR_CORES = CPUS_PER_NODE 

    EXECUTOR_MEMORY = int(
        MEM_PER_NODE / (CPUS_PER_NODE / EXECUTOR_CORES) * 0.5
    )  # set executor memory in GB on each worker node

    # Make sure there is a dir for spark logs
    if not os.path.exists('spark_logs'):
        os.mkdir('spark_logs')

    conf = SparkConf().setAll(
        [
            ("spark.app.name", 'spark_assignment'), # Change to your liking 
            ("spark.sql.caseSensitive", False), # Optional: Make queries strings sensitive to captialization
            ("spark.master", MASTER_HOST),
            ("spark.cores.max", CLUSTER_CORES_MAX),
            ("spark.executor.cores", EXECUTOR_CORES),
            ("spark.executor.memory", str(EXECUTOR_MEMORY) + "g"),
            ("spark.eventLog.enabled", True),
            ("spark.eventLog.dir", "spark_logs"),
            ("spark.history.fs.logDirectory", "spark_logs"),
            ("spark.deploy.mode", "cluster"),
        ]
    )

    ## check executor memory, taking into accout 10% of memory overhead (minimum 384 MiB)
    CHECK = (CLUSTER_CORES_MAX / EXECUTOR_CORES) * (
        EXECUTOR_MEMORY + max(EXECUTOR_MEMORY * 0.10, 0.403)
    )

    assert (
        int(CHECK) <= CLUSTER_MEMORY_MAX
    ), "Executor memory larger than cluster total memory!"

    # Stop previous session that was just for loading cluster params
    spark.stop()

    # Start new session with above config, that has better resource handling
    spark: SparkSession = SparkSession.builder.config(conf=conf)\
                            .getOrCreate()
    sc: SparkContext = spark.sparkContext
    print("Success!")

    return sc, spark


def create_context(local: bool) -> Tuple[SparkContext, SparkSession]:
    global _EXECUTED_
    global sc
    global spark

    if '_EXECUTED_' in globals():
        # check if variable '_EXECUTED_' exists in the global variable namespace
        print("Already been executed once, not running again!")
    else:
        print("Cell has not been executed before, running...")
        if local:
            sc, spark = create_local_context()
        else:
            sc, spark = create_remote_context()

        _EXECUTED_ = True

    sc.setLogLevel("ERROR")

    return sc, spark