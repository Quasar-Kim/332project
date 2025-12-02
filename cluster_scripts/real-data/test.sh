#!/usr/bin/env bash

##### DEFINITIONS #####
MASTER_IP="10.1.25.21" # for workers
WORKER_IPS=("2.2.2.101" "2.2.2.102" "2.2.2.103" "2.2.2.104" "2.2.2.105" "2.2.2.106" "2.2.2.107" "2.2.2.108" "2.2.2.109" "2.2.2.110" "2.2.2.111" "2.2.2.112" "2.2.2.113" "2.2.2.114" "2.2.2.115" "2.2.2.116" "2.2.2.117" "2.2.2.118" "2.2.2.119" "2.2.2.120")
WORKER_SSH_PORT=22
USER="red"
DIR_PROJECT="/home/red/332project"
DIR_OUTPUT="/home/red/dataset_real_output"
DIR_INPUT="/dataset"
DIR_SUMMARIES="/home/red/dataset_real_summaries"
VALSORT_PATH="/home/red/valsort"

# assume code up to date

usage() {
    echo "make sure the code is updated"
    echo "Each command requires the data size ("small", "big", "large") and the number of workers, the rest is optional."
    echo ""
    echo "Run master: <data size> master <# of workers> <port for master; defualt 5000>"
    echo "Run workers: <data size> workers <# of workers> <port for master; defualt 5000> <port for 1st worker; defualt 6001> <port for replicator; defualt 7000>"
    echo "Validate result: <data size> validate <# of workers>"
    echo ""
    echo "Examples:"
    echo "./test.sh small master 2 5050"
    echo "./test.sh small workers 2 5050 6061 7070"
    echo "./test.sh small validate 2"
}

master() {
    # start master
    local n=$1
    local port=$2
    #( cd "$DIR_PROJECT" && sbt "master/run $n --port $port" )
    java -jar /home/red/332project/master/target/scala-2.13/master.jar $n --port $port
}

workers() {
    # create output directories and run workers
    local n=$1
    local master_port=${2:-5000}
    local worker1_port=${3:-6001}
    local replicator_port=${4:-7000}
    for worker in ${WORKER_IPS[@]:0:$n}; do
        echo $worker;

        # output
        ssh $USER@$worker -p $WORKER_SSH_PORT \
            "rm -rf $DIR_OUTPUT/$DATA_SIZE/$n; mkdir -p $DIR_OUTPUT/$DATA_SIZE/$n"

        # run
        ssh -f $USER@$worker -p $WORKER_SSH_PORT \
            "java -jar /home/red/332project/worker/target/scala-2.13/worker.jar $MASTER_IP:$master_port -I $DIR_INPUT/$DATA_SIZE -O $DIR_OUTPUT/$DATA_SIZE/$n --port $worker1_port --replicator-local-port $replicator_port"
    done
}

validate() {
    local n=$1
    mkdir -p "$DIR_SUMMARIES/$DATA_SIZE/$n"
    local count=0 # partition count
    for ((i=0; i<n; i++)); do
        worker=${WORKER_IPS[i]}
        echo $worker;

        # remove previous summaries
        ssh "$USER@$worker" -p "$WORKER_SSH_PORT" \
            "rm -f $DIR_OUTPUT/$DATA_SIZE/$n/partition.*.sum"

        # list all partitions on worker
        partitions=$(ssh "$USER@$worker" -p "$WORKER_SSH_PORT" \
            "ls $DIR_OUTPUT/$DATA_SIZE/$n/partition.* 2>/dev/null")

        # process each partition found
        for path in $partitions; do
            fname=$(basename "$path")
            idx=$(echo "$fname" | sed 's/partition.\([0-9]\+\).*/\1/')
            echo "idx $idx"
            count=$count+1

            # generate summary on worker
            ssh "$USER@$worker" -p "$WORKER_SSH_PORT" \
                "$VALSORT_PATH -o $DIR_OUTPUT/$DATA_SIZE/$n/partition.$idx.sum $DIR_OUTPUT/$DATA_SIZE/$n/partition.$idx"

            # copy summary to master
            scp "$USER@$worker:$DIR_OUTPUT/$DATA_SIZE/$n/partition.$idx.sum" \
                "$DIR_SUMMARIES/$DATA_SIZE/$n"
        done
    done
    # concatenate in order
    > "$DIR_SUMMARIES/$DATA_SIZE/$n/all.sum"
    for ((i=0; i<count; i++)); do
        cat "$DIR_SUMMARIES/$DATA_SIZE/$n/partition.$i.sum" >> "$DIR_SUMMARIES/$DATA_SIZE/$n/all.sum"
    done
    echo ""
    $VALSORT_PATH -s "$DIR_SUMMARIES/$DATA_SIZE/$n/all.sum"
    echo ""
    local records_total=$(( 320000 * $FILES * $n ))
    echo "Expect records: $records_total"
}

case $1 in
    small) DATA_SIZE="small"; FILES=2;;
    big) DATA_SIZE="big"; FILES=10;;
    large) DATA_SIZE="large"; FILES=100;;
    *) usage;;
esac

case $2 in
    master)
        master $3 $4;;
    workers)
        workers $3 $4 $5 $6;;
    validate)
        validate $3;;
    *)
        usage;;
esac