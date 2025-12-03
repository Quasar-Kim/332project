#!/usr/bin/env bash

##### DEFINITIONS #####
MASTER_IP="10.1.25.21" # for workers
WORKER_IPS=("2.2.2.101" "2.2.2.102" "2.2.2.103" "2.2.2.104" "2.2.2.105" "2.2.2.106" "2.2.2.107" "2.2.2.108" "2.2.2.109" "2.2.2.110" "2.2.2.111" "2.2.2.112" "2.2.2.113" "2.2.2.114" "2.2.2.115" "2.2.2.116" "2.2.2.117" "2.2.2.118" "2.2.2.119" "2.2.2.120")
WORKER_SSH_PORT=22
USER="red"
DIR_PROJECT="/home/red/332project"
VALSORT_PATH="/home/red/valsort"

# assume code up to date

usage() {
    echo "make sure the code is updated"
    echo "Execute the commands provided by the edge case scripts."
    echo ""
    echo "Available edge cases:"
    echo "empty.sh -- input contains or entirely consists of empty files/directories"
    echo "size-count.sh -- experimenting with tiny and giant files"
    echo "ranges.sh -- no shufflig or keys on the cusp of ranges"
    echo "skew.sh -- differently skewed or distributed data"
    echo "duplicates.sh -- identical files or files with only identical records"
    echo "binary.sh -- binary data instead of ASCII"
}

master() {
    # start master
    local port=${1:-5000}
    java -jar /home/red/332project/master/target/scala-2.13/master.jar $N --port $port
}

workers() {
    # create output directories and run workers
    local input="$1"
    local output=$2
    local master_port=${3:-5000}
    local worker1_port=${4:-6001}
    local replicator_port=${5:-7000}
    for worker in ${WORKER_IPS[@]:0:$N}; do
        echo $worker;

        # output
        ssh $USER@$worker -p $WORKER_SSH_PORT \
            "rm -rf $output; mkdir -p $output"

        # run
        ssh -f $USER@$worker -p $WORKER_SSH_PORT \
            "java -jar /home/red/332project/worker/target/scala-2.13/worker.jar $MASTER_IP:$master_port -I $input -O $output --port $worker1_port --replicator-local-port $replicator_port"
    done
}

validate() {
    local out_dir=$1
    local sum_dir=$2
    local rec_cnt=$3
    mkdir -p "$sum_dir"
    local count=0 # partition count
    for ((i=0; i<N; i++)); do
        worker=${WORKER_IPS[i]}
        echo $worker;

        # remove previous summaries
        ssh "$USER@$worker" -p "$WORKER_SSH_PORT" \
            "rm -f $out_dir/partition.*.sum"

        # list all partitions on worker
        partitions=$(ssh "$USER@$worker" -p "$WORKER_SSH_PORT" \
            "ls $out_dir/partition.* 2>/dev/null")

        # process each partition found
        for path in $partitions; do
            fname=$(basename "$path")
            idx=$(echo "$fname" | sed 's/partition.\([0-9]\+\).*/\1/')
            echo "idx $idx"
            count=$count+1

            # generate summary on worker
            ssh "$USER@$worker" -p "$WORKER_SSH_PORT" \
                "$VALSORT_PATH -o $out_dir/partition.$idx.sum $out_dir/partition.$idx"

            # copy summary to master
            scp "$USER@$worker:$out_dir/partition.$idx.sum" \
                "$sum_dir"
        done
    done
    # concatenate in order
    > "$sum_dir/all.sum"
    for ((i=0; i<count; i++)); do
        cat "$sum_dir/partition.$i.sum" >> "$sum_dir/all.sum"
    done
    echo ""
    $VALSORT_PATH -s "$sum_dir/all.sum"
    echo ""
    echo "Expect records: $rec_cnt"
}

case $1 in
    master)
        N=$2
        master $3
        ;;
    workers)
        N=$2
        workers "$3" $4 $5 $6 $7
        ;;
    validate)
        N=$2
        validate $3 $4 $5
        ;;
    *)
        usage;;
esac