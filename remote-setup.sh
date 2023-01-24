#Variables
CLUSTER_NAME="mas433-nonesp-wasb-707"
SSH_USER="sshuser"


read -sp 'SSH Password: ' SSH_PASSWORD
read -sp 'Ambari Password: ' AMBARI_PASSWORD


sshpass -p $SSH_PASSWORD ssh $SSH_USER@$CLUSTER_NAME-ssh.azurehdinsight.net "bash -s" <./prep-and-run.sh $AMBARI_PASSWORD
