#!/bin/bash

#Variables
CLUSTER_NAME=$1
IS_ESP=$2
SSH_USER=$3
SSH_PASSWORD=$4
AMBARI_USER=$5
AMBARI_PASSWORD=$6

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
sshpass -vvv -p $SSH_PASSWORD ssh $SSH_USER@$CLUSTER_NAME-ssh.azurehdinsight.net "bash -s" $CLUSTER_NAME $AMBARI_USER $AMBARI_PASSWORD $IS_ESP $SSH_USER <./prep-and-run.sh
