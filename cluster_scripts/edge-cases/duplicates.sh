##### DEFINITIONS #####
WORKER_IPS=("2.2.2.101" "2.2.2.102" "2.2.2.103" "2.2.2.104" "2.2.2.105" "2.2.2.106" "2.2.2.107" "2.2.2.108" "2.2.2.109" "2.2.2.110" "2.2.2.111" "2.2.2.112" "2.2.2.113" "2.2.2.114" "2.2.2.115" "2.2.2.116" "2.2.2.117" "2.2.2.118" "2.2.2.119" "2.2.2.120")
WORKER_SSH_PORT=22
USER="red"
DIR_PROJECT="/home/red/332project"
DIR_OUTPUT="/home/red/edge_case_output/duplicates"
DIR_INPUT="/home/red/edge_case_input/duplicates"
DIR_SUMMARIES="/home/red/edge_case_summaries/duplicates"
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

##### FILE SETS #####
sets() {
    local files=$1
    local records=$2

    local inp_parent="$DIR_INPUT/$TYPE/$N-machines/$DIRS-dirs-$files-files-$records-recs"
    local out_dir="$DIR_OUTPUT/$TYPE/$N-machines/$DIRS-dirs-$files-files-$records-recs"

    local worker1=${WORKER_IPS[0]}
    # make/clear the input parent and output directory on vm01
    ssh $USER@$worker1 -p $WORKER_SSH_PORT \
        "rm -rf $inp_parent; mkdir -p $inp_parent; rm -rf $out_dir; mkdir -p $out_dir"
    # generate the file set on vm01
    for ((i=0; i<DIRS; i++)); do
        ssh $USER@$worker1 -p $WORKER_SSH_PORT \
            "mkdir -p $inp_parent/dir$i"
        for ((j=0; j<files; j++)); do
            ssh $USER@$worker1 -p $WORKER_SSH_PORT \
                "$GENSORT_PATH -a $records $inp_parent/dir$i/file$j"
        done
    done

    for worker in ${WORKER_IPS[@]:1:$((N-1))}; do
        echo $worker;

        # make/clear the input parent and output directory
        ssh $USER@$worker -p $WORKER_SSH_PORT \
            "rm -rf $inp_parent; mkdir -p $inp_parent; rm -rf $out_dir; mkdir -p $out_dir"

        # copy the dataset from vm01 (by directory)
        for ((i=0; i<DIRS; i++)); do
            scp -r -P $WORKER_SSH_PORT $USER@$worker1:$inp_parent/dir$i $USER@$worker:$inp_parent
        done
    done

    # make the input directory string
    dir_string=""
    for ((i=0; i<DIRS; i++)); do
        dir_string+="$inp_parent/dir$i "
    done
    # remove trailing space
    dir_string=${dir_string% }

    local rec_cnt=$(( $records * $files * $DIRS * $N ))
    
    generate_commands "$dir_string" "$out_dir" "$DIR_SUMMARIES/$TYPE/$N-machines/$DIRS-dirs-$files-files-$records-recs" $rec_cnt
}

##### FILES #####
files() {
    local files=$1
    local records=$2

    local inp_parent="$DIR_INPUT/$TYPE/$N-machines/$DIRS-dirs-$files-files-$records-recs"
    local out_dir="$DIR_OUTPUT/$TYPE/$N-machines/$DIRS-dirs-$files-files-$records-recs"

    local worker1=${WORKER_IPS[0]}
    # make/clear the input parent and output directory on vm01
    ssh $USER@$worker1 -p $WORKER_SSH_PORT \
        "rm -rf $inp_parent; mkdir -p $inp_parent/dir0; rm -rf $out_dir; mkdir -p $out_dir"
    # generate the file on vm01
    local path0="$inp_parent/dir0/file0"
    ssh $USER@$worker1 -p $WORKER_SSH_PORT \
        "$GENSORT_PATH -a $records $path0"
    # make duplicates of the file
    for ((i=0; i<DIRS; i++)); do
        ssh $USER@$worker1 -p $WORKER_SSH_PORT \
            "mkdir -p $inp_parent/dir$i"
        for ((j=0; j<files; j++)); do
            if [[ $i -ne 0 || $j -ne 0 ]]; then
                ssh $USER@$worker1 -p $WORKER_SSH_PORT \
                    "cp $path0 $inp_parent/dir$i/file$j"
            fi
        done
    done

    for worker in ${WORKER_IPS[@]:1:$((N-1))}; do
        echo $worker;

        # make/clear the input parent and output directory
        ssh $USER@$worker -p $WORKER_SSH_PORT \
            "rm -rf $inp_parent; mkdir -p $inp_parent; rm -rf $out_dir; mkdir -p $out_dir"

        # copy the dataset from vm01 (by directory)
        for ((i=0; i<DIRS; i++)); do
            scp -r -P $WORKER_SSH_PORT $USER@$worker1:$inp_parent/dir$i $USER@$worker:$inp_parent
        done
    done

    # make the input directory string
    dir_string=""
    for ((i=0; i<DIRS; i++)); do
        dir_string+="$inp_parent/dir$i "
    done
    # remove trailing space
    dir_string=${dir_string% }

    local rec_cnt=$(( $records * $files * $DIRS * $N ))
    
    generate_commands "$dir_string" "$out_dir" "$DIR_SUMMARIES/$TYPE/$N-machines/$DIRS-dirs-$files-files-$records-recs" $rec_cnt
}

##### MIX #####
mix() {
    local set_copies=$1
    local iden_files=$2
    local diff_files=$3
    local records=$4

    local files=$(( $iden_files + $diff_files ))

    local inp_parent="$DIR_INPUT/$TYPE/$N-machines/$DIRS-dirs-$1-sc-$2-if-$3-df-$4-recs"
    local out_dir="$DIR_OUTPUT/$TYPE/$N-machines/$DIRS-dirs-$1-sc-$2-if-$3-df-$4-recs"

    local worker1=${WORKER_IPS[0]}
    # make/clear the input parent and output directory on vm01
    ssh $USER@$worker1 -p $WORKER_SSH_PORT \
        "rm -rf $inp_parent; mkdir -p $inp_parent/dir0; rm -rf $out_dir; mkdir -p $out_dir"
    # generate the file on vm01
    local path0="$inp_parent/dir0/file0"
    ssh $USER@$worker1 -p $WORKER_SSH_PORT \
        "$GENSORT_PATH -a $records $path0"
    # make duplicates of the file
    for ((i=0; i<DIRS; i++)); do
        ssh $USER@$worker1 -p $WORKER_SSH_PORT \
            "mkdir -p $inp_parent/dir$i"
        for ((j=0; j<files; j++)); do
            if [[ $j -lt $iden_files ]]; then
                if [[ $i -ne 0 || $j -ne 0 ]]; then
                    ssh $USER@$worker1 -p $WORKER_SSH_PORT \
                        "cp $path0 $inp_parent/dir$i/file$j"
                fi
            else
                ssh $USER@$worker1 -p $WORKER_SSH_PORT \
                    "$GENSORT_PATH -a $records $inp_parent/dir$i/file$j"
            fi
        done
    done

    for (( w=1; w<N; w++ )); do
        local worker=${WORKER_IPS[$w]}
        echo $worker;

        # make/clear the input parent and output directory
        ssh $USER@$worker -p $WORKER_SSH_PORT \
            "rm -rf $inp_parent; mkdir -p $inp_parent; rm -rf $out_dir; mkdir -p $out_dir"

        if [[ $w -lt $set_copies ]]; then
            # copy the dataset from vm01 (by directory)
            for ((i=0; i<DIRS; i++)); do
                scp -r -P $WORKER_SSH_PORT $USER@$worker1:$inp_parent/dir$i $USER@$worker:$inp_parent
            done
        else
            for ((i=0; i<DIRS; i++)); do
                ssh $USER@$worker -p $WORKER_SSH_PORT \
                    "mkdir -p $inp_parent/dir$i"
                for ((j=0; j<files; j++)); do
                    ssh $USER@$worker -p $WORKER_SSH_PORT \
                        "$GENSORT_PATH -a $records $inp_parent/dir$i/file$j"
                done
            done
        fi
    done

    # make the input directory string
    dir_string=""
    for ((i=0; i<DIRS; i++)); do
        dir_string+="$inp_parent/dir$i "
    done
    # remove trailing space
    dir_string=${dir_string% }

    local rec_cnt=$(( $records * $files * $DIRS * $N ))
    
    generate_commands "$dir_string" "$out_dir" "$DIR_SUMMARIES/$TYPE/$N-machines/$DIRS-dirs-$1-sc-$2-if-$3-df-$4-recs" $rec_cnt
}

usage() {
    echo "Each command creates the needed directories and files, and outputs the commands to perform the test"
    echo ""
    echo "Identical file sets: ./duplicates.sh sets <# workers> <# directories> <files per directory> <records per file>"
    echo "Identical files: ./duplicates.sh files <# workers> <# directories> <files per directory> <records per file>"
    echo "Mix: ./duplicates.sh mix <# workers> <# set copies> <# directories> <# file copies> <# other files> <records per file>"
}

case $1 in
    sets)
        N=$2
        TYPE=$1
        DIRS=$3
        sets $4 $5
        ;;
    files)
        N=$2
        TYPE=$1
        DIRS=$3
        files $4 $5
        ;;
    mix)
        N=$2
        TYPE=$1
        DIRS=$4
        mix $3 $5 $6 $7
        ;;
    *) usage;;
esac