# Instructions for a Multiple Hosts setup

In this example we will use two hosts, the first host that we will call "Master Host" and another host that we will call "Slave Host".
We will also call `<master-ip>` the local IP address of the Master Host.

Note: In many of the commands it is required to run as root using `sudo` because Spark needs to correctly bind to multiple ports to allow communication between the hosts.

Note: If you encounter any problem try to disable the firewall on the machines.


### Step 0: Download Spark (on both hosts)

Download Apache Spark from [here](https://spark.apache.org/downloads.html)

The application has been tested using [Spark release 3.0.2 pre-built for Apache Hadoop 2.7](https://www.apache.org/dyn/closer.lua/spark/spark-3.0.2/spark-3.0.2-bin-hadoop2.7.tgz).


### Step 1: Set custom paths (on both hosts)

Modify these variables based on your setup on both the Master Host and the Slave Host.

* `export SPARK_FOLDER_PATH=/Users/Desno365/Developing-software/spark-3.0.2-bin-hadoop2.7/`
* `export PROJECT_FOLDER_PATH=/Users/Desno365/Desktop/Politecnico/Corsi-e-materiale/Middleware-for-Distributed-Systems/Projects/project5-spark/`

Note:
  * `SPARK_FOLDER_PATH` is the path to the folder containing Spark that has been downloaded at Step 0.
  * `PROJECT_FOLDER_PATH` is the path to the folder containing this project, these instructions themselves are contained in the folder.


### Step 2: Setup the Spark configuration (on both hosts)

* `cd $SPARK_FOLDER_PATH`
* Create or modify the file `./conf/spark-defaults.conf` by setting these 3 properties:
    * `spark.master spark://<master-ip>:7077`
    * `spark.eventLog.enabled true`
    * `spark.eventLog.dir /tmp/spark-events/`
* Create or modify the file `./conf/spark-env.sh` by setting these 2 properties:
    * `SPARK_MASTER_HOST=<master-ip>`
    * `SPARK_MASTER_PORT=7077`


### Step 3: Prepare the Spark configuration (on both hosts)

* `mkdir /tmp/spark-events`
* `cp -R $PROJECT_FOLDER_PATH /tmp/spark-files/`
 

### Step 4: Start Master (ONLY on the Master Host)

* `sudo ${SPARK_FOLDER_PATH}sbin/start-master.sh`

Now it's possible to access the Spark Web UI of the master at [127.0.0.1:8080](http://127.0.0.1:8080).


### Step 5: Start Slave (ONLY on the Slave Host)

* `sudo ${SPARK_FOLDER_PATH}sbin/start-slave.sh spark://<master-ip>:7077`

Now it's possible to access the Spark Web UI of the slave at [127.0.0.1:8081](http://127.0.0.1:8081).


### Step 6: Start History Server (ONLY on the Master Host)

* `sudo ${SPARK_FOLDER_PATH}sbin/start-history-server.sh`

Now it's possible to access the Spark Web UI of the History Server at [127.0.0.1:18080](http://127.0.0.1:18080).


### Step 7: Run the Spark application (ONLY on the Master Host)

Note: The application must have already been compiled to a Jar (use Maven package).

* `sudo ${SPARK_FOLDER_PATH}bin/spark-submit --class it.polimi.middleware.spark.Main ${PROJECT_FOLDER_PATH}target/project5-spark-1.2.jar spark://<master-ip>:7077 /tmp/spark-files/ all true true`

Note about arguments:
1) First parameter: address of the Spark Master.
2) Second parameter: path to the project folder.
3) Third parameter: type `ecdc` or `simulation` to chose between the possible datasets, or `all` to run the analysis on all the datasets.
4) Fourth parameter: type `true` to use cache, `false` to not use cache.
4) Fifth parameter: type `true` to show results also on the terminal, `false` to not show them.


### Step 8: Stop Master, Slave and History Server

When the application has completed its execution it is possible to stop all the Spark daemons:

* On the Master Host run: `sudo ${SPARK_FOLDER_PATH}sbin/stop-history-server.sh`
* On the Slave Host run: `sudo ${SPARK_FOLDER_PATH}sbin/stop-slave.sh`
* On the Master Host run: `sudo ${SPARK_FOLDER_PATH}sbin/stop-master.sh`
