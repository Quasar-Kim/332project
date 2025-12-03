##### DEFINITIONS #####
MASTER_IP="141.223.16.227"
MASTER_SSH_PORT=7777
WORKER_IPS=("2.2.2.101" "2.2.2.102" "2.2.2.103" "2.2.2.104" "2.2.2.105" "2.2.2.106" "2.2.2.107" "2.2.2.108" "2.2.2.109" "2.2.2.110" "2.2.2.111" "2.2.2.112" "2.2.2.113" "2.2.2.114" "2.2.2.115" "2.2.2.116" "2.2.2.117" "2.2.2.118" "2.2.2.119" "2.2.2.120")
WORKER_SSH_PORT=22
USER="red"
DIR_PROJECT="/home/red/332project"
GIT_REPO="https://github.com/Quasar-Kim/332project"
VALSORT_PATH="/home/red/valsort"

##### UPDATE PROJECT #####
update_master() {
    echo "[START] updating project code on master"
    # compile jobs and master
    cd 332project && git checkout main && git pull && sbt jobs/compile && sbt master/compile && sbt master/assembly
    echo "[DONE]"
}
update_workers() {
    local n=$1
    echo "[START] updating, compiling, and assembling project code on $n workers"
    for worker in ${WORKER_IPS[@]:0:$n}; do
        echo $worker;
        # update
        ssh -f $USER@$worker -p $WORKER_SSH_PORT \
            "cd $DIR_PROJECT && git checkout main && git pull && sbt jobs/compile && sbt worker/compile && sbt worker/assembly && echo $worker done"
    done
    echo "[DONE]"
}
update_specific_worker() {
    local n=$1
    echo "[START] updating, compiling, and assembling project code on worker $n"
    worker=${WORKER_IPS[$(( n - 1 ))]}
    echo $worker
    # update
    ssh $USER@$worker -p $WORKER_SSH_PORT \
        "cd $DIR_PROJECT && git checkout main && git pull && sbt jobs/compile && sbt worker/compile && sbt worker/assembly"
    echo "[DONE]"
}

##### INITIALIZE A WORKER #####
init_specific_worker() {
    local n=$1
    echo "[START] initializing worker $n (establishing a connection, cloning the repository)"
    worker=${WORKER_IPS[$(( n - 1 ))]}
    echo $worker
    # update
    ssh $USER@$worker -p $WORKER_SSH_PORT \
        "git clone $GIT_REPO"
    # add valsort
    scp -P $WORKER_SSH_PORT $VALSORT_PATH $worker:/home/$USER
    echo "[DONE]"
}

usage() {
    echo "Commands:"
    echo "1) $0 update <# workers> -- updates, compiles, and assembles code on master and the specified number of workers"
    echo "2) $0 update_specific <idx of worker> -- updates, compiles, and assembles code on the specified worker, i.e., vm0[idx] or vm[idx]"
    echo "3) $0 init_specific <idx of workers> -- initializes the specified worker (clones the repository, copies valsort, compiles, and assembles)"
    echo ""
    echo "Number of workers: 1-20"
    echo ""
    echo "Examples:"
    echo "1) $0 update 3 -- will update master, vm01, vm02, and vm03"
    echo "2) $0 update_specific 4 -- will update vm04"
    echo "3) $0 init_specific 5 -- will initialize and update vm05"
}

case $1 in
  update)
    update_master
    update_workers $2 # # of workers
    ;;
  update_specific)
    update_specific_worker $2 # idx of worker
    ;;
  init_specific)
    init_specific_worker $2 # idx of worker
    update_specific_worker $2 # idx of worker
    ;;
  *)
    usage
    ;;
esac