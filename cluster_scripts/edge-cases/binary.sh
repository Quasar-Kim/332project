##### DEFINITIONS #####
WORKER_IPS=("2.2.2.101" "2.2.2.102" "2.2.2.103" "2.2.2.104" "2.2.2.105" "2.2.2.106" "2.2.2.107" "2.2.2.108" "2.2.2.109" "2.2.2.110" "2.2.2.111" "2.2.2.112" "2.2.2.113" "2.2.2.114" "2.2.2.115" "2.2.2.116" "2.2.2.117" "2.2.2.118" "2.2.2.119" "2.2.2.120")
WORKER_SSH_PORT=22
USER="red"
DIR_PROJECT="/home/red/332project"
DIR_OUTPUT="/home/red/edge_case_output/binary"
DIR_INPUT="/home/red/edge_case_input/binary"
DIR_SUMMARIES="/home/red/edge_case_summaries/binary"
GENSORT_PATH="/home/red/gensort"

generate_commands() {
    local inp_str=$1
    local out_str=$2
    local val_str=$3
    local record_count=$4
    echo "Execute the test with the following commands."
    echo ""
    echo "Master (optionally add port at the end):"
    echo "    ./test.sh master $N"
    echo ""
    echo "Workers (optionally add ports for master, 1st worker, and replicator at the end):"
    echo "    ./test.sh workers $N \"$inp_str\" \"$out_str\""
    echo ""
    echo "Validation:"
    echo "    ./test.sh validate $N \"$out_str\" \"$val_str\" $record_count"

}

##### GENERATE DATA #####
data() {
    local inp_parent="$DIR_INPUT/$N-machines/$DIRS-dirs-$FILES-files-$RECORDS-recs"
    local out_dir="$DIR_OUTPUT/$N-machines/$DIRS-dirs-$FILES-files-$RECORDS-recs"

    for worker in ${WORKER_IPS[@]:0:$N}; do
        echo $worker;

        # make/clear the input parent and output directory
        ssh $USER@$worker -p $WORKER_SSH_PORT \
            "rm -rf $inp_parent; mkdir -p $inp_parent; rm -rf $out_dir; mkdir -p $out_dir"

        # make the actual empty input directory/directories
        for ((i=0; i<DIRS; i++)); do
            ssh $USER@$worker -p $WORKER_SSH_PORT \
                "mkdir -p $inp_parent/dir$i"
            for ((j=0; j<FILES; j++)); do
                ssh $USER@$worker -p $WORKER_SSH_PORT \
                    "$GENSORT_PATH $RECORDS $inp_parent/dir$i/file$j"
            done
        done
    done

    # make the input directory string
    dir_string=""
    for ((i=0; i<DIRS; i++)); do
        dir_string+="$inp_parent/dir$i "
    done
    # remove trailing space
    dir_string=${dir_string% }

    local rec_cnt=$(( $DIRS * $FILES * $RECORDS * $N ))

    generate_commands "$dir_string" "$out_dir" "$DIR_SUMMARIES/$TYPE/$N-machines/$DIRS-dirs-$FILES-files-$RECORDS-recs" $rec_cnt
}

usage() {
    echo "The command creates the needed directories and files, and outputs the commands to perform the test"
    echo ""
    echo "Usage: ./binary.sh <# workers> <# directories> <# files per directory> <# records per file>"
    echo ""
    echo "The created files will contain binary records, not ASCII."
}

case $1 in
    [1-9]|1[0-9]|20)
        N=$1
        DIRS=$2
        FILES=$3
        RECORDS=$4
        data
        ;;
    *) usage;;
esac