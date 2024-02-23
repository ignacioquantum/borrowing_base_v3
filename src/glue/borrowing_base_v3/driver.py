import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.conf import SparkConf

from src.glue.borrowing_base_v3.borrowing_base_v3 import BorrowingBaseV3

if __name__ == "__main__":
    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "ARN_ROLE_REDSHIFT", "ENV", "LOG_GROUP_NAME",
                   "LOG_APP_NAME", "secret_name", "year", "month", "day", "fx"]
    )

    conf = (SparkConf().setApp("BorrowingBaseV3")
            .set("spark.driver.extraClassPath",
                 "/home/mysql-connector-j-8.3.0.jar"))
    sc = SparkContext(conf=conf)

    glue: GlueContext = GlueContext(sc)
    spark = glue.spark_session
    job: Job = Job(glue)

    driver = BorrowingBaseV3(spark, args)

    job.init(args["JOB_NAME"], args)
    driver.main()

    job.commit()
