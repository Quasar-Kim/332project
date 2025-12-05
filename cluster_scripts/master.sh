#!/bin/bash

MASTER_JAR=/home/red/332project/master/target/scala-2.13/master.jar

usage() {
    echo "Usage:"
    echo "    cluster_scripts/master.sh <num_workers> [-p <port>]"
    echo ""
    echo "Arguments:"
    echo "    num_workers    <-- number of worker machines (1-20)"
    echo "    -p port        <-- optional master port (5000 by default)"
    echo ""
    echo "Examples:"
    echo "cluster_scripts/deploy.sh master 5"
    echo "cluster_scripts/deploy.sh master 4 -p 5004"
    echo ""
    echo "Make sure the code is updated before deployment."
    exit 1
}

# no arguments
[[ $# -eq 0 ]] && usage

# invalid number of machines
case $1 in
    [1-9]|1[0-9]|20) N=$1;;
    *)
        echo "Invalid number of worker machines"
        usage;;
esac

shift

PORT=5000

while getopts ":p:" opt; do
    case "$opt" in
        p)
            if [[ ! "$OPTARG" =~ ^[0-9]+$ ]] || (( OPTARG < 1 || OPTARG > 65535 )); then
                echo "Invalid port: $OPTARG"
                exit 1
            fi
            PORT="$OPTARG";;
        \?)
            echo "Unknown option: -$OPTARG"
            usage;;
        :)
            echo "Option -$OPTARG requires an argument"
            usage;;
    esac
done

java -jar $MASTER_JAR $N --port $PORT