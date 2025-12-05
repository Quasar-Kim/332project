#!/bin/bash

WORKER_JAR=/home/red/332project/worker/target/scala-2.13/worker.jar

usage() {
    echo "Usage:"
    echo "    cluster_scripts/worker.sh <IP>:<m_port> -I <inputs...> -O <output> [-p <w_port>] [-r <r_port>]"
    echo ""
    echo "Arguments:"
    echo "    IP          <-- master's IP"
    echo "    m_port      <-- master's port"
    echo "    -I inputs   <-- input directories"
    echo "    -O output   <-- output directory"
    echo "    -p w_port   <-- optional port for 1st worker thread (6001 by default)"
    echo "    -r r_port   <-- optional port for local replicator (7000 by default)"
    echo ""
    echo "Examples:"
    echo "cluster_scripts/deploy.sh worker 10.1.25.21:5000 -I /home/dir0 /home/dir2 -O /home/output"
    echo "cluster_scripts/deploy.sh worker 10.1.25.21:5000 -I /home/in -O /home/out -p 6002 -r 7001"
    echo ""
    echo "Make sure the code is updated before deployment."
    exit 1
}

# no arguments
[[ $# -eq 0 ]] && usage

MASTER_ADDR=$1
shift

# validate address format
if [[ ! "$MASTER_ADDR" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:[0-9]+$ ]]; then
    echo "Master address must be in IP:port format"
    usage
fi

# parse address
MASTER_IP="${MASTER_ADDR%%:*}"
M_PORT="${MASTER_ADDR##*:}"
if [[ ! "$M_PORT" =~ ^[0-9]+$ ]] || (( M_PORT < 1 || M_PORT > 65535 )); then
    echo "Invalid master port: $M_PORT"
    exit 1
fi

W_PORT=6001
R_PORT=7000
INPUTS=()
OUTPUT=""

expecting=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        -I)
            expecting=true
            shift
            continue;;
        -O)
            OUTPUT="$2"
            if [[ -z "$OUTPUT" ]]; then
                echo "-O requires an argument"
                usage
            fi
            shift 2
            continue;;
        -p)
            W_PORT="$2"
            if [[ ! "$W_PORT" =~ ^[0-9]+$ ]] || (( W_PORT < 1 || W_PORT > 65535 )); then
                echo "Invalid 1st worker port: $W_PORT"
                exit 1
            fi
            shift 2
            continue;;
        -r)
            R_PORT="$2"
            if [[ ! "$R_PORT" =~ ^[0-9]+$ ]] || (( R_PORT < 1 || R_PORT > 65535 )); then
                echo "Invalid local replicator port: $R_PORT"
                exit 1
            fi
            shift 2
            continue;;
        -*)
            echo "Unknown option: $1"
            usage;;
        *)
            if $expectng; then
                INPUTS+=("$1") # adding input directories
                shift
                continue
            else
                echo "Unexpected argument: $1"
                usage
            fi;;
    esac
done

# ensure directories are present
if [[ ${#INPUTS[@]} -eq 0 ]]; then
    echo "Input directories are required"
    usage
fi
if [[ -z "$OUTPUT" ]]; then
    echo "Output directory is required"
    usage
fi

# make the input directory string
input_str=""
for dir in ${INPUTS[@]}; do
    input_str+="$dir "
done
# remove trailing space
input_str=${input_str% }

java -jar $WORKER_JAR $MASTER_IP:$M_PORT -I $input_str -O $OUTPUT --port $W_PORT --replicator-local-port $R_PORT