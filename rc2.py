import subprocess
subprocess.run(["spark-submit","--driver-memory","512m","--conf","spark.ui.enabled=false","--conf","spark.sql.shuffle.partitions=4","--conf","spark.driver.maxResultSize=128m","--packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3","consumer.py"])
