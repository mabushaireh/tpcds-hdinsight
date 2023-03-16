#!/bin/bash

#Variables
CLUSTER_NAME=$1
IS_ESP=$2
SSH_USER=$3
SSH_PASSWORD=$4
AMBARI_USER=$5
AMBARI_PASSWORD=$6
PORT=$9
EXECUTE_QUERY=$10
GGENERATE_TABLE=$11



if [ $CLUSTER_NAME = "" ]; then
    read -p 'Cluster DNS Name: ' CLUSTER_NAME
    echo ""
fi

if [ $CLUSTER_NAME = "" ]; then

    read -p 'Is ESP (Y or N): ' IS_ESP
    echo ""
fi
if [ $CLUSTER_NAME = "" ]; then
    read -p 'SSH User: ' SSH_USER
    echo ""
fi
if [ $CLUSTER_NAME = "" ]; then
    read -sp 'SSH Password: ' SSH_PASSWORD
    echo ""
fi
if [ $CLUSTER_NAME = "" ]; then
    read -p 'Ambari User: ' AMBARI_USER
    echo ""
fi
if [ $CLUSTER_NAME = "" ]; then
    read -sp 'Ambari Password: ' AMBARI_PASSWORD
    echo ""
fi
sshpass -p $SSH_PASSWORD ssh -p $PORT $SSH_USER@$CLUSTER_NAME-ssh.azurehdinsight.net "bash -s" --  <./prep-and-run.sh -f $7 -c $8 -h $CLUSTER_NAME -u $AMBARI_USER -p $AMBARI_PASSWORD -s $IS_ESP -q $EXECUTE_QUERY -g $GGENERATE_TABLE
