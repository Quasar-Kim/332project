##### DEFINITIONS #####
WORKER_IPS=("2.2.2.101" "2.2.2.102" "2.2.2.103" "2.2.2.104" "2.2.2.105" "2.2.2.106" "2.2.2.107" "2.2.2.108" "2.2.2.109" "2.2.2.110" "2.2.2.111" "2.2.2.112" "2.2.2.113" "2.2.2.114" "2.2.2.115" "2.2.2.116" "2.2.2.117" "2.2.2.118" "2.2.2.119" "2.2.2.120")
WORKER_SSH_PORT=22
USER="red"
DIR_PROJECT="/home/red/332project"
DIR_OUTPUT="/home/red/edge_case_output/empty"
DIR_INPUT="/home/red/edge_case_input/empty"
DIR_SUMMARIES="/home/red/edge_case_summaries/empty"
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

##### DIRECTORY #####
directory() {
    local n_dirs=$1

    local inp_parent="$DIR_INPUT/$TYPE/$N-machines/$n_dirs-dir"
    local out_dir="$DIR_OUTPUT/$TYPE/$N-machines/$n_dirs-dir"

    for worker in ${WORKER_IPS[@]:0:$N}; do
        echo $worker;

        # make/clear the input parent and output directory
        ssh $USER@$worker -p $WORKER_SSH_PORT \
            "rm -rf $inp_parent; mkdir -p $inp_parent; rm -rf $out_dir; mkdir -p $out_dir"

        # make the actual empty input directory/directories
        for ((i=0; i<n_dirs; i++)); do
            echo "$inp_parent/dir$i"
            ssh $USER@$worker -p $WORKER_SSH_PORT \
                "mkdir -p $inp_parent/dir$i"
        done
    done

    # make the input directory string
    dir_string=""
    for ((i=0; i<n_dirs; i++)); do
        dir_string+="$inp_parent/dir$i "
    done
    # remove trailing space
    dir_string=${dir_string% }

    generate_commands "$dir_string" "$out_dir" "$DIR_SUMMARIES/$TYPE/$N-machines/$n_dirs-dir" 0
}

##### FILE #####
file() {
    local n_dirs=$1
    local n_files=$2

    local inp_parent="$DIR_INPUT/$TYPE/$N-machines/$n_dirs-$n_files"
    local out_dir="$DIR_OUTPUT/$TYPE/$N-machines/$n_dirs-$n_files"

    for worker in ${WORKER_IPS[@]:0:$N}; do
        echo $worker;

        # make/clear the input parent and output directory
        ssh $USER@$worker -p $WORKER_SSH_PORT \
            "rm -rf $inp_parent; mkdir -p $inp_parent; rm -rf $out_dir; mkdir -p $out_dir"

        # make the actual empty input directory/directories and the empty files
        for ((i=0; i<n_dirs; i++)); do
            echo "dir$i"
            ssh $USER@$worker -p $WORKER_SSH_PORT \
                "mkdir -p $inp_parent/dir$i"
            for ((j=0; j<n_files; j++)); do
                echo "file$j"
                ssh $USER@$worker -p $WORKER_SSH_PORT \
                    "> $inp_parent/dir$i/file$j"
            done
        done
    done

    # make the input directory string
    dir_string=""
    for ((i=0; i<n_dirs; i++)); do
        dir_string+="$inp_parent/dir$i "
    done
    # remove trailing space
    dir_string=${dir_string% }
    
    generate_commands "$dir_string" "$out_dir" "$DIR_SUMMARIES/$TYPE/$N-machines/$n_dirs-$n_files" 0
}

##### MIX #####
mix() {
    local empty_dirs=$1
    local filled_dirs=$2
    local empty_files=$3
    local filled_files=$4
    local records=$5
    
    local n_dirs=$(( $empty_dirs + $filled_dirs ))
    local n_files=$(( $empty_files + $filled_files ))
    echo "$n_dirs input directories in total"

    local inp_parent="$DIR_INPUT/$TYPE/$N-machines/$1ed-$2fd-$3ef-$4ff-$5r"
    local out_dir="$DIR_OUTPUT/$TYPE/$N-machines/$1ed-$2fd-$3ef-$4ff-$5r"

    for worker in ${WORKER_IPS[@]:0:$N}; do
        echo $worker;

        # make/clear the input parent and output directory
        ssh $USER@$worker -p $WORKER_SSH_PORT \
            "rm -rf $inp_parent; mkdir -p $inp_parent; rm -rf $out_dir; mkdir -p $out_dir"

        # make the actual input directory/directories and files
        for ((i=0; i<n_dirs; i++)); do
            ssh $USER@$worker -p $WORKER_SSH_PORT \
                "mkdir -p $inp_parent/dir$i"
            if [[ $i -lt $filled_dirs ]]; then
                for ((j=0; j<n_files; j++)); do
                    if [[ $j -lt $filled_files ]]; then
                        ssh $USER@$worker -p $WORKER_SSH_PORT \
                            "> $inp_parent/dir$i/file$j"
                    else
                        ssh $USER@$worker -p $WORKER_SSH_PORT \
                            "$GENSORT_PATH -a $records $inp_parent/dir$i/file$j"
                    fi
                done
            fi
        done
    done

    # make the input directory string
    dir_string=""
    for ((i=0; i<n_dirs; i++)); do
        dir_string+="$inp_parent/dir$i "
    done
    # remove trailing space
    dir_string=${dir_string% }

    local rec_cnt=$(( $records * $filled_files * $filled_dirs * $N ))
    
    generate_commands "$dir_string" "$out_dir" "$DIR_SUMMARIES/$TYPE/$N-machines/$n_dirs-$n_files" $rec_cnt
}

usage() {
    echo "Each command creates the needed directories and files, and outputs the commands to perform the test"
    echo ""
    echo "Empty directories: ./empty.sh dir <# workers> <# directories>"
    echo "Empty files: ./empty.sh file <# workers> <# directories> <# files per directory>"
    echo "Mix: ./empty.sh mix <# workers> <# empty directories> <# non-empty directories> <# empty files> <# non-empty files> <records per file>"
}

case $1 in
    dir)
        N=$2
        TYPE=$1
        directory $3
        ;;
    file)
        N=$2
        TYPE=$1
        file $3 $4
        ;;
    mix)
        N=$2
        TYPE=$1
        mix $3 $4 $5 $6 $7
        ;;
    *) usage;;
esac