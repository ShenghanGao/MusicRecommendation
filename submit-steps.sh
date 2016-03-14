#!/bin/bash
################################################################################
#  A simple Python based example for Spark
#  Designed to run on SDSC's Comet resource.
#  T. Yang. Modified from Mahidhar Tatineni's scripe for scala
################################################################################
#SBATCH --job-name="musicRecomm"
#SBATCH --output="musicRecomm.%j.%N.out"
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=24
#SBATCH --export=ALL
#SBATCH -t 00:30:00

### Environment setup for Hadoop and Spark
module load spark
export PATH=/opt/hadoop/2.6.0/sbin:$PATH
export HADOOP_CONF_DIR=$HOME/mycluster.conf
export WORKDIR=`pwd`

myhadoop-configure.sh

### Start HDFS.  Starting YARN isn't necessary since Spark will be running in
### standalone mode on our cluster.
start-dfs.sh

### Load in the necessary Spark environment variables
source $HADOOP_CONF_DIR/spark/spark-env.sh

### Start the Spark masters and workers.  Do NOT use the start-all.sh provided 
### by Spark, as they do not correctly honor $SPARK_CONF_DIR
myspark start

### Copy the data into HDFS
hdfs dfs -mkdir -p /user/$USER

hdfs dfs -put $WORKDIR/$1 /user/$USER/data.txt
hdfs dfs -put $WORKDIR/$2 /user/$USER/normalized_user.txt

echo "STEP2"
spark-submit artist_user_matrix_text.py /user/$USER/data.txt
echo "STEP3"
spark-submit normalize_ratings.py filtered_user_matrix.out/part-00000
echo "STEP4"
spark-submit uaLinesToAuLine.py normalized_ratings.out/part-00000
echo "STEP5"
time spark-submit co_pearson.py ua_lines_to_au_line.out/part-00000
echo "STEP6"
spark-submit recomm_weighed.py /user/$USER/normalized_user.txt /user/$USER/artist_avg.out/part-00000 co_matrix.out/part-00000

#copy out
hadoop dfs -copyToLocal recomm.out $WORKDIR

### Shut down Spark and HDFS
myspark stop
stop-dfs.sh

### Clean up
myhadoop-cleanup.sh

