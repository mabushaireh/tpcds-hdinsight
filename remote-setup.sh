#Variables
read -p 'Cluster DNS Name: ' CLUSTER_NAME
echo ""

read -p 'Is ESP (Y or N): ' IS_ESP
echo ""

read -p 'SSH User: ' SSH_USER
echo ""
read -sp 'SSH Password: ' SSH_PASSWORD
echo ""
read -p 'Ambari User: ' AMBARI_USER
echo ""
read -sp 'Ambari Password: ' AMBARI_PASSWORD
echo ""


sshpass -p $SSH_PASSWORD ssh $SSH_USER@$CLUSTER_NAME-ssh.azurehdinsight.net "bash -s" <./prep-and-run.sh $CLUSTER_NAME $AMBARI_USER $AMBARI_PASSWORD $IS_ESP
