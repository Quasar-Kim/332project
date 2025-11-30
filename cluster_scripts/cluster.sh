##### DEFINITIONS #####
MASTER_IP="141.223.16.227"
MASTER_SSH_PORT=7777
WORKER_IPS=("2.2.2.101" "2.2.2.102" "2.2.2.103" "2.2.2.104" "2.2.2.105" "2.2.2.106" "2.2.2.107" "2.2.2.108" "2.2.2.109" "2.2.2.110" "2.2.2.111" "2.2.2.112" "2.2.2.113" "2.2.2.114" "2.2.2.115" "2.2.2.116" "2.2.2.117" "2.2.2.118" "2.2.2.119" "2.2.2.120")
WORKER_SSH_PORT=22
USER="red"
DIR_PROJECT="/home/red/332project"
# TODO customize directories (?)
DIRS_INPUT=("/home/red/input1" "/home/red/input2")
DIR_OUTPUT="/home/red/data"
GIT_REPO="https://github.com/Quasar-Kim/332project"
GENSORT_PATH="/home/red/gensort"
VALSORT_PATH="/home/red/valsort"

##### UPDATE PROJECT #####
update_master() {
    echo "[START] updating project code on master"
    git clone $GIT_REPO 2>/dev/null || echo 'already cloned'
    # compile jobs and master
    cd 332project && git pull origin main && sbt jobs/compile && sbt master/compile
    echo "[DONE]"
}
update_workers() {
    local n=$1
    echo "[START] updating project code on $n workers"
    for worker in ${WORKER_IPS[@]:0:$n}; do
        echo $worker;
        # clone
        ssh $USER@$worker -p $WORKER_SSH_PORT \
            "clone $GIT_REPO 2>/dev/null || echo 'already cloned'"
        # update
        ssh $USER@$worker -p $WORKER_SSH_PORT \
            "cd $DIR_PROJECT && git pull origin main && sbt jobs/compile && sbt worker/compile"
    done
    echo "[DONE]"
}

##### GENERATE DATA #####
init_gensort() {
    local n=$1
    echo "[START] initializing gensort on master"
    if [ -f $GENSORT_PATH ]; then
        echo "gensort already initialized"
    else
        cd /home/$USER
        wget http://www.ordinal.com/try.cgi/gensort-linux-1.5.tar.gz # TODO put the binaries on git?
        tar -xzf gensort-linux-1.5.tar.gz
        cp /home/$USER/64/* /home/$USER # gensort and valsort
    fi
    echo "[DONE]"
    echo "[START] copying gensort to $n workers"
    for worker in ${WORKER_IPS[@]:0:$n}; do
        echo $worker;
        scp -P $WORKER_SSH_PORT $GENSORT_PATH $worker:/home/$USER
        scp -P $WORKER_SSH_PORT $VALSORT_PATH $worker:/home/$USER
    done
    echo "[DONE]"
}
make_dirs() {
    local n=$1
    echo "[START] making input and output directories on $n workers"
    for worker in ${WORKER_IPS[@]:0:$n}; do
        echo $worker;

        # output
        # delete old directory
        ssh $USER@$worker -p $WORKER_SSH_PORT \
            "rm -rf $DIR_OUTPUT"
        # make new directory
        ssh $USER@$worker -p $WORKER_SSH_PORT \
            "mkdir -p $DIR_OUTPUT"

        # input
        for dir_inp in ${DIRS_INPUT[@]}; do
            # delete old directory
            ssh $USER@$worker -p $WORKER_SSH_PORT \
                "rm -rf $dir_inp"
            # make new directory
            ssh $USER@$worker -p $WORKER_SSH_PORT \
                "mkdir -p $dir_inp"
        done
    done
    echo "[DONE]"
}
# TODO different number of records per file, different number of files per worker
generate_data() {
    local n=$1 # number of workers
    local records=$2 # to generate on each worker
    #local ascii=$3 # 1=ascii 0=binary # TODO?
    # TODO add a start for the records?
    local n_files=$3
    echo "[START] generating $records records to $n_files files on $n workers"
    for worker in ${WORKER_IPS[@]:0:$n}; do
        echo $worker;
        for ((i=1; i<=n_files; i++)); do
            for dir_inp in ${DIRS_INPUT[@]}; do
                path="$dir_inp/data$i"
                # delete old record
                ssh $USER@$worker -p $WORKER_SSH_PORT \
                    "if [ -f $path ]; then rm $path; fi"
                # generate new record
                ssh $USER@$worker -p $WORKER_SSH_PORT \
                    "$GENSORT_PATH $records $path"
            done
        done
    done
    echo "[DONE]"
}

##### START MASTER #####
start_master() {
    local n=$1
    echo "[START] starting master for $n workers"
    ( cd "$DIR_PROJECT" && sbt "master/run $n" )
}

##### START WORKERS #####
start_workers() {
    local n=$1
    local master_ip=$2
    local master_port=$3
    # TODO different input directories per worker
    input_dirs_str=$(IFS=" "; echo "${DIRS_INPUT[*]}") # all same on each worker for now
    echo input_dirs_str
    echo "[START] starting $n workers"
    for worker in ${WORKER_IPS[@]:0:$n}; do
        echo $worker;
        # clear the output directory
        ssh $USER@$worker -p $WORKER_SSH_PORT \
            "rm -rf $DIR_OUTPUT/*"
        # start worker
        ssh $USER@$worker -p $WORKER_SSH_PORT \
            "( cd $DIR_PROJECT; sbt 'worker/run $master_ip:$master_port -I $input_dirs_str -O $DIR_OUTPUT' )"
    done
    echo "[DONE]"
}

##### CHECK RESULTS #####
# TODO

##### ENTIRE PROJECT #####
# update all, generate data, run master
prepare() {
    local n=$1
    local records=$2 # to generate on each worker
    local n_files=$3 # to generate in input directories
    update_master
    update_workers $n
    init_gensort $n
    make_dirs $n
    generate_data $n $records $n_files
}

usage() {
    echo "Commands:"
    echo "1) $0 prepare <# workers> <# records per files> <# files per worker> -- updates code, generates data"
    echo "2) $0 start_master <# workers>"
    echo "3) $0 start_workers <# workers> -- should be done after starting the master"
    echo ""
    echo "Number of workers: 1-20"
    echo ""
    echo "Examples:"
    echo "1) $0 prepare 3 10000 2 -- will generate 10000 in 2 files on 3 worker machnines"
}

case $1 in
  prepare)
    prepare $2 $3 $4 # # of workers, # of records to generate in each file, # of files to generate on each worker
    ;;
  start_master)
    start_master $2 # number of workers
    ;;
  start_workers)
    start_workers $2 "10.1.25.21" 5000 # # of workers
    ;;
  *) # Default case
    usage
    ;;
esac