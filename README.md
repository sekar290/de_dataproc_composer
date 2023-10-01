# Submit Spark Job to DataProc Cluster

Notes:
This project suppose to use cloud to run, but since I dont have the account cloud, here is some steps based on my understanding:

1. Create a cloud Dataproc Cluster
2. Uploading python code to cloud that can be access by Dataproc cluster. (lets say I have put it into this path: 'gs://path/to/your/wordCount.py')
3. Run the wordcount code using DataProcSparkOperator
4. Delete Cloud Dataproc cluster