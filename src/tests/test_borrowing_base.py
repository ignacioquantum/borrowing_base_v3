from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark_assert import assert_frame_equal

from src.glue.borrowing_base_v3.borrowing_base_v3 import BorrowingBaseV3
from src.glue.borrowing_base_v3.config import ENV_VAR, BORROWING_TABLES

def assert_frame_not_equal(*args, **kwargs):
    try:
        assert_frame_equal(*args, **kwargs)
    except AssertionError:
        # frames are not equal
        pass
    else:
        # frames are equal
        raise AssertionError


def test_p_subquery() -> None:

    spark = SparkSession.builder \
        .appName("BorrowingBase") \
        .getOrCreate()

    args: dict = {
        "ENV": "dev",
        "year": "2023",
        "month": "11",
        "day": "30",
        "fx": 17.2333,
        "status": ['ACTIVE', 'BOOKED', 'CURRENT', 'TERMINATED'],
        "status_detail_ignore": ['RESTRUCTURED', 'SPECIAL CONDITIONS'],
        "lease_id_list": None
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
        "fx": 17.2333,
        "status": ['ACTIVE', 'BOOKED', 'CURRENT', 'TERMINATED'],
        "status_detail_ignore": ['RESTRUCTURED', 'SPECIAL CONDITIONS'],
        "lease_id_list": None
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
        "fx": 17.2333,
        "status": ['ACTIVE', 'BOOKED', 'CURRENT', 'TERMINATED'],
        "status_detail_ignore": ['RESTRUCTURED', 'SPECIAL CONDITIONS'],
        "lease_id_list": None
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


def test_all_id_final_query() -> None:
    spark = SparkSession.builder \
        .appName("BorrowingBase") \
        .getOrCreate()

    args: dict = {
        "ENV": "dev",
        "year": "2023",
        "month": "11",
        "day": "30",
        "fx": 17.2333,
        "status" : ['ACTIVE', 'BOOKED', 'CURRENT', 'TERMINATED'],
        "status_detail_ignore" : ['RESTRUCTURED', 'SPECIAL CONDITIONS'],
        "lease_id_list": None
    }
    configs: dict = ENV_VAR[args['ENV']]

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
                      withColumn('Borrowing_base_as_of_fx_Date',
                                 col('Borrowing_base_as_of_fx_Date').cast('double'))).
                     withColumn('Borrowing_base_as_of_Closing_FX',
                                col('Borrowing_base_as_of_Closing_FX').cast('double'))).
                    withColumn('Borrowing_base_curr_as_of_fx_Date',
                               col('Borrowing_base_curr_as_of_fx_Date').cast('double'))).
                   withColumn('Borrowing_base_curr_as_of_Closing_FX',
                              col('Borrowing_base_curr_as_of_Closing_FX').cast('double'))).
                  withColumn('Season', col('Season').cast('double'))).
                 withColumn('Life', col('Life').cast('double'))).
                withColumn('Closing_Date', to_date(col('Closing_Date'), 'dd/MM/yy')))

    work_dfs: dict = {}

    for table, properties in BORROWING_TABLES.items():
        work_dfs[table] = spark.read.csv(properties["test_path"], header=True)

    work_dfs['leaseparameters'] = work_dfs['leaseparameters'].withColumn("l.ClosingDate",
                                                                         to_date(col("`l.ClosingDate`"),
                                                                                 'dd/MM/yy')
                                                                         )

    final_test = BorrowingBaseV3(spark, args).get_final_df(work_dfs, dm, sub1x,configs)

    assert_frame_equal(final_df, final_test)
    spark.stop()


def test_specific_id_final_query() -> None:
    spark = SparkSession.builder \
        .appName("BorrowingBase") \
        .getOrCreate()

    args: dict = {
        "ENV": "dev",
        "year": "2023",
        "month": "11",
        "day": "30",
        "fx": 17.2333,
        "status" : ['ACTIVE', 'BOOKED', 'CURRENT', 'TERMINATED'],
        "status_detail_ignore" : ['RESTRUCTURED', 'SPECIAL CONDITIONS'],
        "lease_id_list": [2, 33]
    }

    configs: dict = ENV_VAR[args['ENV']]

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
                withColumn('Borrowing_base_as_of_fx_Date',
                           col('Borrowing_base_as_of_fx_Date').cast('double'))).
                withColumn('Borrowing_base_as_of_Closing_FX',
                           col('Borrowing_base_as_of_Closing_FX').cast('double'))).
                withColumn('Borrowing_base_curr_as_of_fx_Date',
                           col('Borrowing_base_curr_as_of_fx_Date').cast('double'))).
                withColumn('Borrowing_base_curr_as_of_Closing_FX',
                           col('Borrowing_base_curr_as_of_Closing_FX').cast('double'))).
                withColumn('Season', col('Season').cast('double'))).
                withColumn('Life', col('Life').cast('double'))).
                withColumn('Closing_Date', to_date(col('Closing_Date'), 'dd/MM/yy')))

    work_dfs: dict = {}

    for table, properties in BORROWING_TABLES.items():
        work_dfs[table] = spark.read.csv(properties["test_path"], header=True)

    work_dfs['leaseparameters'] = work_dfs['leaseparameters'].withColumn("l.ClosingDate",
                                                                         to_date(col("`l.ClosingDate`"),
                                                                                 'dd/MM/yy')
                                                                         )

    final_test = BorrowingBaseV3(spark, args).get_final_df(work_dfs, dm, sub1x,configs)

    assert_frame_not_equal(final_df, final_test)

    final_df = final_df.select('*').where(col('Lease_ID').isin(args['lease_id_list']))

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
