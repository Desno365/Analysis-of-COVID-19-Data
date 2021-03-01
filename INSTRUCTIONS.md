# Instructions


### Step 0: Download Spark

It is possible to download Apache Spark from [here](https://spark.apache.org/downloads.html)

The application has been tested using [Spark release 3.0.2 pre-built for Apache Hadoop 2.7](https://www.apache.org/dyn/closer.lua/spark/spark-3.0.2/spark-3.0.2-bin-hadoop2.7.tgz).


### Step 1: Set custom paths

* `export SPARK_FOLDER_PATH=/Users/Desno365/Developing-software/spark-3.0.2-bin-hadoop2.7`
* `export SPARK_PROJECT_FOLDER=/Users/Desno365/Desktop/Politecnico/Corsi-e-materiale/Middleware-for-Distributed-Systems/Projects/project5-spark`


### Step 2: Modify the Spark configuration

* `export SPARK_LOCAL_IP=127.0.0.1`
* `export SPARK_MASTER_HOST=127.0.0.1`
* `cd $SPARK_FOLDER_PATH`
* `mkdir /tmp/spark-events`
* Change or create the file `/conf/spark-defaults.conf` by setting these 3 properties:
    * `spark.master spark://127.0.0.1:7077`
    * `spark.eventLog.enabled true`
    * `spark.eventLog.dir /tmp/spark-events/`
   
 
### Step 3: Start the master

* `./sbin/start-master.sh`

Now it's possible to access the Spark Web UI of the master at [127.0.0.1:8080](127.0.0.1:8080)


### Step 4: Start a slave

* `./sbin/start-slave.sh spark://127.0.0.1:7077`

Now it's possible to access the Spark Web UI of the slave at [127.0.0.1:8081](127.0.0.1:8081)


### Step 5: Start History Server

* `./sbin/start-history-server.sh`

Now it's possible to access the Spark Web UI of the History Server at [127.0.0.1:18080](127.0.0.1:18080)


### Step 6: Run the Spark application

Note: The application must have already been compiled to a Jar (use Maven package).

* `./bin/spark-submit --class it.polimi.middleware.spark.Main $SPARK_PROJECT_FOLDER/target/project5-spark-1.0.jar spark://127.0.0.1:7077 $SPARK_PROJECT_FOLDER/ false`


### Step 7: Stop master, slave and History Server

When the application has completed its execution it is possible to stop all the Spark daemons:

* `./sbin/stop-history-server.sh`
* `./sbin/stop-slave.sh`
* `./sbin/stop-master.sh`
