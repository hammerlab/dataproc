# dataproc
Simple python script for running a Spark job on an ephemeral Google Cloud dataproc cluster.

```bash
$ ./dataproc -h
usage: dataproc [-h] [--cluster CLUSTER] [--timestamp-cluster-name]
                [--cores CORES] [--properties PROPS_FILES] [--jar JAR]
                [--main MAIN] [--machine-type MACHINE_TYPE] [--dry-run]

Run a Spark job on an ephemeral dataproc cluster

optional arguments:
  -h, --help            show this help message and exit
  --cluster CLUSTER     Name of the dataproc cluster to use; defaults to
                        $CLUSTER env var
  --timestamp-cluster-name, -t
                        When true, append "-<TIMESTAMP>" to the dataproc
                        cluster name
  --cores CORES         Number of CPU cores to use
  --properties PROPS_FILES, -p PROPS_FILES
                        Comma-separated list of Spark properties files; merged
                        with $SPARK_PROPS_FILES env var
  --jar JAR             URI of main app JAR; defaults to JAR env var
  --main MAIN, -m MAIN  JAR main class; defaults to MAIN env var
  --machine-type MACHINE_TYPE
                        Machine type to use
  --dry-run, -n         When set, print some of the parsed and inferred
                        arguments and exit without running any dataproc
                        commands
```

See [hammerlab/pageant scripts/run-on-gcloud](https://github.com/hammerlab/pageant/blob/e337a48d4eb643919a24779e0080c8c7dde463c6/scripts/run-on-gcloud) for an example use that simply sets `MAIN`, `CLUSTER`, and `JAR` env vars and delegates to this script.
