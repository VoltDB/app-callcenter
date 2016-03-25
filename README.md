Call Center Example Application
==============

Idempotent processing.

Streaming Joins.


Quickstart
--------------
VoltDB Examples come with a run.sh script that sets up some environment and saves some of the typing needed to work with Java clients. It should be fairly readable to show what is precisely being run to accomplish a given task.

1. Make sure "bin" inside the VoltDB kit is in your path.
2. Type "voltdb create --force" to start an empty, single-node VoltDB server.
3. Type "sqlcmd < ddl.sql" to load the schema and the jarfile of procedures into VoltDB.
4. Type "./run.sh client" to run the client code.

The default settings for the client have it keep 30 seconds worth of tuples, deleting older tuples as an ongoing process. See the section below on *run.sh Client Options* for how to run in other modes.

Note that the downloaded VoltDB kits include pre-compiled stored procedures and client code as jarfiles. To run the example from a source build, it may be necessary to compile the Java source code by typing "run.sh jars" before step 3 above. Note that this step requires a full Java JDK.


Other run.sh Actions
--------------
- *run.sh* : start the server
- *run.sh server* : start the server
- *run.sh init* : load the schema and stored procedures
- *run.sh jars* : compile all Java clients and stored procedures into two Java jarfiles
- *run.sh client* : start the client
- *run.sh clean* : remove compilation and runtime artifacts
- *run.sh cleanall* : remove compilation and runtime artifacts *and* the two included jarfiles


run.sh Client Options
--------------
Near the bottom of the run.sh bash script is the section run when you type `run.sh client`. In that section is the actual shell command to run the client code, reproduced below:

    java -classpath client:$CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        windowing.WindowingApp \
        --displayinterval=5 \              # how often to print the report
        --duration=120 \                   # how long to run for
        --servers=localhost:21212          # servers to connect to


Changing these settings changes the behavior of the app.
