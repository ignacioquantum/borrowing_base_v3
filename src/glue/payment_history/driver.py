import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from src.glue.payment_history.payment_history import PaymentHistory

if __name__ == "__main__":
    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "ARN_ROLE_REDSHIFT", "ENV", "LOG_GROUP_NAME", "LOG_APP_NAME", "secret_name"]
    )
    sc = SparkContext()

    glue: GlueContext = GlueContext(sc)
    spark = glue.spark_session
    job: Job = Job(glue)

    driver = PaymentHistory(spark, args)

    job.init(args["JOB_NAME"], args)
    driver.main()

    job.commit()
