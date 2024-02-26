from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark_assert import assert_frame_equal

from src.glue.borrowing_base_v3.borrowing_base_v3 import BorrowingBaseV3
from src.glue.borrowing_base_v3.config import BORROWING_TABLES


def test_p_subquery() -> None:

    spark = SparkSession.builder \
        .appName("BorrowingBase") \
        .getOrCreate()

    args: dict = {
        "ENV": "dev",
        "year": "2023",
        "month": "11",
        "day": "30",
        "fx": 17.2333
    }

    p = spark.read.csv("resources/idb_subqueries/p_subquery_test.csv", header=True)
    p = p.withColumn('total_payment', col('total_payment').cast('double'))
    p = p.withColumn('total_payment_vat', col('total_payment_vat').cast('double'))
    p = p.withColumn('RV', col('RV').cast('double'))

    work_dfs: dict = {}

    for table, properties in BORROWING_TABLES.items():
        work_dfs[table] = spark.read.csv(properties["test_path"], header=True)

    p_test = BorrowingBaseV3(spark, args).get_subquery_p(work_dfs)

    assert_frame_equal(p_test, p, check_row_order=False)

    spark.stop()


def test_sub1x_subquery() -> None:

    spark = SparkSession.builder \
        .appName("BorrowingBase") \
        .getOrCreate()

    args: dict = {
        "ENV": "dev",
        "year": "2023",
        "month": "11",
        "day": "30",
        "fx": 17.2333
    }

    sub1x = spark.read.csv("resources/idb_subqueries/sub1x_test.csv", header=True)
    sub1x = sub1x.withColumn('ppmt_due', col('ppmt_due').cast('double'))
    sub1x = sub1x.withColumn('vat_due', col('vat_due').cast('double'))
    sub1x = sub1x.withColumn('rv_due', col('rv_due').cast('double'))
    sub1x = sub1x.withColumn('curr_dmd_prin', col('curr_dmd_prin').cast('double'))
    work_dfs: dict = {}

    for table, properties in BORROWING_TABLES.items():
        work_dfs[table] = spark.read.csv(properties["test_path"], header=True)

    p_test = BorrowingBaseV3(spark, args).get_subquery_p(work_dfs)
    sub1x_test = BorrowingBaseV3(spark, args).get_subquery_sub1x(p_test)

    assert_frame_equal(sub1x, sub1x_test, check_row_order=False)
    spark.stop()


def test_dm_subquery() -> None:
    spark = SparkSession.builder \
        .appName("BorrowingBase") \
        .getOrCreate()

    args: dict = {
        "ENV": "dev",
        "year": "2023",
        "month": "11",
        "day": "30",
        "fx": 17.2333
    }

    dm = spark.read.csv("resources/idb_subqueries/dm_subquery_test.csv", header=True)
    dm = dm.withColumn('dmd_prin', col('dmd_prin').cast('double'))
    dm = dm.withColumn('prin_no_vat', col('prin_no_vat').cast('double'))
    dm = dm.withColumn('prin_vat', col('prin_vat').cast('double'))

    work_dfs: dict = {}

    for table, properties in BORROWING_TABLES.items():
        work_dfs[table] = spark.read.csv(properties["test_path"], header=True)

    dm_test = BorrowingBaseV3(spark, args).get_subquery_dm(work_dfs)

    assert_frame_equal(dm, dm_test, check_row_order=False)
    spark.stop()


def test_final_query() -> None:
    spark = SparkSession.builder \
        .appName("BorrowingBase") \
        .getOrCreate()

    args: dict = {
        "ENV": "dev",
        "year": "2023",
        "month": "11",
        "day": "30",
        "fx": 17.2333
    }

    dm = spark.read.csv("resources/idb_subqueries/dm_subquery_test.csv", header=True)
    dm = dm.withColumn('dmd_prin', col('dmd_prin').cast('double'))
    dm = dm.withColumn('prin_no_vat', col('prin_no_vat').cast('double'))
    dm = dm.withColumn('prin_vat', col('prin_vat').cast('double'))

    sub1x = spark.read.csv("resources/idb_subqueries/sub1x_test.csv", header=True)
    sub1x = sub1x.withColumn('ppmt_due', col('ppmt_due').cast('double'))
    sub1x = sub1x.withColumn('vat_due', col('vat_due').cast('double'))
    sub1x = sub1x.withColumn('rv_due', col('rv_due').cast('double'))
    sub1x = sub1x.withColumn('curr_dmd_prin', col('curr_dmd_prin').cast('double'))

    final_df = spark.read.csv("resources/idb_subqueries/final_df_test.csv", header=True)
    final_df = (((((((final_df.withColumn('DRAW', col('DRAW').cast('double')).
                withColumn('Borrowing base as of fx Date',
                           col('Borrowing base as of fx Date').cast('double'))).
                withColumn('Borrowing base as of Closing FX',
                           col('Borrowing base as of Closing FX').cast('double'))).
                withColumn('Borrowing base curr as of fx Date',
                           col('Borrowing base curr as of fx Date').cast('double'))).
                withColumn('Borrowing base curr as of Closing FX',
                           col('Borrowing base curr as of Closing FX').cast('double'))).
                withColumn('Season', col('Season').cast('double'))).
                withColumn('Life', col('Life').cast('double'))).
                withColumn('Closing Date', to_date(col('Closing Date'), 'dd/MM/yy')))

    work_dfs: dict = {}

    for table, properties in BORROWING_TABLES.items():
        work_dfs[table] = spark.read.csv(properties["test_path"], header=True)

    work_dfs['leaseparameters'] = work_dfs['leaseparameters'].withColumn("l.ClosingDate",
                                                                         to_date(col("`l.ClosingDate`"),
                                                                                 'dd/MM/yy')
                                                                         )

    final_test = BorrowingBaseV3(spark, args).get_final_df(work_dfs, dm, sub1x)

    assert_frame_equal(final_df, final_test)
    spark.stop()


def test_columns_new_names():
    spark = SparkSession.builder \
        .appName("BorrowingBase") \
        .getOrCreate()

    work_dfs: dict = {}

    for table, properties in BORROWING_TABLES.items():
        work_dfs[table] = spark.read.csv(properties["test_path"], header=True)
        for column in work_dfs[table].columns:
            assert column.startswith(properties['alias'])
