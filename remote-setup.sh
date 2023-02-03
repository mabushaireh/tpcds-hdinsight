#Variables
CLUSTER_NAME="mas906-esp-adlsgen2-707"
SSH_USER="sshuser"


read -sp 'SSH Password: ' SSH_PASSWORD
echo ""
read -sp 'Ambari Password: ' AMBARI_PASSWORD
echo ""


sshpass -p $SSH_PASSWORD ssh $SSH_USER@$CLUSTER_NAME-ssh.azurehdinsight.net "bash -s" <./prep-and-run.sh $AMBARI_PASSWORD
