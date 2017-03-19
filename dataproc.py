#!/usr/bin/env python

from argparse import ArgumentParser
from math import ceil
from os import environ
import re
from subprocess import check_call
import sys
from time import time

def env(key, default=None):
    return environ.get(key, default)

parser = ArgumentParser(description = "Run a Spark job on an ephemeral dataproc cluster")

parser.add_argument(
    '--cluster', dest='cluster',
    default=env('CLUSTER'),
    help='Name of the dataproc cluster to use; defaults to $CLUSTER env var'
)

parser.add_argument(
    '--timestamp-cluster-name', '-t', dest='timestamp_cluster',
    action='store_true',
    help='When true, append "-<TIMESTAMP>" to the dataproc cluster name'
)

parser.add_argument(
    '--cores', dest='cores',
    type=int,
    default=env('CORES', 200),
    help='Number of CPU cores to use'
)

parser.add_argument(
    '--properties', '-p', dest='props_files',
    default='',
    help='Comma-separated list of Spark properties files; merged with $SPARK_PROPS_FILES env var'
)

parser.add_argument(
    '--jar', dest='jar',
    default=env('JAR'),
    help='URI of main app JAR; defaults to JAR env var'
)

parser.add_argument(
    '--main', '-m', dest='main',
    default=env('MAIN'),
    help='JAR main class; defaults to MAIN env var'
)

parser.add_argument(
    '--machine-type', dest='machine_type',
    default='n1-standard-4',
    help='Machine type to use'
)

parser.add_argument(
    '--dry-run', '-n',
    action='store_true',
    help='When set, print some of the parsed and inferred arguments and exit without running any dataproc commands'
)

args, other_args = parser.parse_known_args(sys.argv[1:])

if not args.jar:
    raise Exception('Required: --jar option or JAR env var')

if not args.main:
    raise Exception('Required: --main option or MAIN env var')

if not args.cluster:
    raise Exception('Required: --cluster option or CLUSTER env var')

machine_type = args.machine_type

match = re.match('^.*?(\d+)$', machine_type)
if not match:
    raise Exception('Malformed machine type? %s' % machine_type)

cores_per_machine = int(match.group(1))

total_num_workers = int(ceil(args.cores / cores_per_machine))

num_workers = 2  # Dataproc's minimum number of non-preemptibile workers

num_preemtible_workers = max(0, total_num_workers - num_workers)

cluster = (
    '%s-%d' % (args.cluster, int(time()))
    if args.timestamp_cluster
    else args.cluster
)

first_whitespace_re = '^(.+?)\s+(.+)$'
def spark_prop_line_to_string(line, prefix):
    if not line:
        return ''
    match = re.match(first_whitespace_re, line)
    if not match:
        raise Exception('Bad line: %s' % line)

    return '%s%s=%s' % (prefix, match.group(1), match.group(2))

def spark_props_to_string(props_file, prefix=''):
    with open(props_file, 'r') as fd:
        return ','.join(
            filter(
                lambda entry: entry,
                [
                    spark_prop_line_to_string(line.strip(), prefix)
                    for line
                    in fd.readlines()
                ]
            )
        )

spark_props_files = (
    args.props_files.split(',')
    if args.props_files
    else []
) + (
    env('SPARK_PROPS_FILES', '').split(',')
    if env('SPARK_PROPS_FILES', '')
    else []
)

spark_props_args = (
    [
        '--properties',
        ','.join(
            [
                spark_props_to_string(props_file)
                for props_file
                in spark_props_files
            ]
        )
    ]
    if spark_props_files
    else []
)

if args.dry_run:
    print('Dry run: commands will only be printed to stdout')

print(
    "Setting up cluster '%s' with %d workers and %d pre-emptible workers" %
    (
        cluster,
        num_workers,
        num_preemtible_workers
    )
)

def run(cmd):
    print("+%s" % ' '.join(cmd))
    if not args.dry_run:
        check_call(cmd)

run(
    [
        "gcloud", "dataproc", "clusters", "create", cluster,
        "--master-machine-type", machine_type,
        "--worker-machine-type", machine_type,
        "--num-workers", str(num_workers),
        "--num-preemptible-workers", str(num_preemtible_workers)
    ]
)

try:
    print("Submitting job")
    run(
        [
            "gcloud", "dataproc", "jobs", "submit", "spark",
            "--cluster", cluster,
            "--class", args.main,
            "--jars", args.jar
        ] +
        spark_props_args +
        [ '--' ] +
        other_args
    )
finally:
    print("Tearing down cluster")
    run(
        [
            "gcloud", "dataproc", "clusters", "delete", cluster
        ]
    )


