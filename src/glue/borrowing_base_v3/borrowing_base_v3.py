import paramiko
from sshtunnel import SSHTunnelForwarder
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, expr, round as spark_round, when, sum as spark_sum, concat_ws, lit, upper, datediff, current_timestamp

from src.glue.borrowing_base_v3.config import ENV_VAR, BORROWING_TABLES


class BorrowingBaseV3:

    def __init__(self, spark: SparkSession, config_arg: dict):
        self.spark = spark
        self.config_arg = config_arg

    def get_subquery_p(self, work_dfs: dict) -> DataFrame:
        p_subquery = work_dfs['leaseparameters'].join(
            work_dfs['totalleasecf'],
            col("`t.idLease`") == col("`l.idLeaseParameters`")
        )

        p_subquery = p_subquery.join(
            work_dfs['vat'],
            (col("`v.idCF`") == col("`t.idTotalLeaseCF`")) &
            (col("`v.idLease`") == col("`t.idLease`")),
            'left'
        )

        p_subquery = p_subquery.join(
            work_dfs['invoice'],
            (col("`i.cashflow_id`") == col("`t.idTotalLeaseCF`")) &
            (col("`i.lease_id`") == col("`l.idLeaseParameters`")) &
            (col("`i.status_id`").isin([1, 3, 4]))
            ,
            'left'
        )

        p_subquery = p_subquery.join(
            work_dfs['payment'],
            (col("`p.invoice_id`") == col("`i.id`")) &
            (col("`p.status_id`").isin([1, 3])) &
            (col("`p.payment_date`") <= concat_ws('-',
                                                  lit(self.config_arg['year']),
                                                  lit(self.config_arg['month']),
                                                  lit(self.config_arg['day'])
                                                  ))
            ,
            'left'
        )

        p_subquery = p_subquery.select(
            '*'
        ).where(
            (col("`t.re_sync_status`") == 'Active') &
            (col("`t.is_initial`") == 0)
        ).groupBy(
            col("`l.idLeaseParameters`"),
            col("`l.LeaseName`"),
            col("`l.DRAW`"),
            col("`t.dMonth`"),
            col("`t.IPMT`"),
            col("`t.PPMT`"),
            col("`t.PMT`"),
            when(col("`t.is_residualvalue`") == 1, 0).otherwise(col("`v.VATKept`")).alias('VATKept'),
            col("`l.Currency`"),
            col("`l.FX`"),
            (col("`t.ResidualVal`") + col("`t.BuyOut`")).alias('RV'),
            col("`t.is_residualvalue`"),
            col("`i.is_initial`"),
            col("`t.dDate`"),
        ).agg(
            spark_sum(when(col("`p.amount`").isNull(), 0).otherwise(
                when(col("`i.is_initial`") == 1, col("`t.PMT`")).otherwise(col("`p.amount`")))).alias("total_payment"),
            spark_sum(when(col("`p.amount_vat`").isNull(), 0).otherwise(
                when(col("`i.is_initial`") == 1, col("`t.PMT`") + col("`v.VATKept`")).otherwise(
                    col("`p.amount_vat`")))).alias("total_payment_vat")
        ).select(
            col("`l.idLeaseParameters`").alias('idLeaseParameters'),
            col("`l.LeaseName`").alias('LeaseName'),
            col("`l.DRAW`").alias('DRAW'),
            col("`t.dMonth`").alias('dMonth'),
            col("`t.IPMT`").alias('IPMT'),
            col("`t.PPMT`").alias('PPMT'),
            col("`t.PMT`").alias('PMT'),
            col("VATKept"),
            col("`l.Currency`").alias('Currency'),
            col("`l.FX`").alias('FX'),
            col("RV"),
            col("`t.is_residualvalue`").alias('is_residualvalue'),
            col("`i.is_initial`").alias('is_initial'),
            col("`t.dDate`").alias('dDate'),
            col("`total_payment`"),
            col("`total_payment_vat`")
        )

        return p_subquery

    def get_subquery_sub1x(self, p_subquery: DataFrame) -> DataFrame:

        sub1x = p_subquery.alias('p').select(
            'p.*',
            when(col('p.total_payment') - col('p.total_payment_vat') > 0,
                 when(col('p.total_payment') - col('p.total_payment_vat') >= col('p.IPMT'), 0).otherwise(
                     col('p.IPMT') - col('p.total_payment') - col('p.total_payment_vat'))).otherwise(
                col('p.IPMT')).alias('ipmt_due'),
            when(col('p.total_payment') - col('p.total_payment_vat') > 0,
                 when(col('p.total_payment') - col('p.total_payment_vat') - col('p.IPMT') < 0, col('p.PPMT')).otherwise(
                     col('p.PPMT') - when(col('p.is_residualvalue') == 0,
                                          col('p.total_payment') - col('p.total_payment_vat') - col(
                                              'p.IPMT')).otherwise(0))).otherwise(col('p.PPMT')).alias('ppmt_due'),
            when(col('p.total_payment_vat') > 0,
                 when(col('p.total_payment_vat') >= col('p.VATKept'), 0).otherwise(
                     col('p.VATKept') - col('p.total_payment_vat'))).otherwise(col('p.VATKept')).alias('vat_due'),
            when(col('p.total_payment') - col('p.total_payment_vat') > 0,
                 when(col('p.total_payment') - col('p.total_payment_vat') >= col('p.RV'), 0).otherwise(
                     col('p.RV') - col('p.total_payment') - col('p.total_payment_vat'))).otherwise(col('p.RV')).alias(
                'rv_due')
        )

        sub1x = sub1x.alias('sub').groupBy(
            'sub.idLeaseParameters',
            'sub.Currency',
            'sub.FX'
        ).agg(
            spark_round(spark_sum('sub.ppmt_due')).alias('ppmt_due'),
            spark_round(spark_sum('sub.vat_due')).alias('vat_due'),
            spark_round(spark_sum('sub.rv_due')).alias('rv_due'),
            spark_round(spark_sum('sub.ppmt_due') + spark_sum('sub.vat_due') + spark_sum('sub.rv_due')).alias(
                'curr_dmd_prin'),
        ).select('*')

        return sub1x

    def get_subquery_dm(self, work_dfs: dict) -> DataFrame:

        dm_subquery = work_dfs['totalleasecf'].join(
            work_dfs['vat'],
            col("`v.idCF`") == col("`t.idTotalLeaseCF`"),
            'left'
        )

        dm_subquery = dm_subquery.where(
            (col("`t.re_sync_status`") == 'Active') &
            (col("`t.is_initial`") == 0)
        ).groupby(
            "`t.idLease`"
        ).agg(
            (spark_sum(col("`t.PPMT`")) + spark_sum(col("`t.ResidualVal`")) + spark_sum(col("`t.BuyOut`")) + spark_sum(
                col("`v.VATKept`"))).alias('dmd_prin'),
            (spark_sum(col("`t.PPMT`")) + spark_sum(col("`t.ResidualVal`")) + spark_sum(col("`t.BuyOut`"))).alias(
                'prin_no_vat'),
            spark_sum(col("`v.VATKept`")).alias('prin_vat')
        ).select(
            col("`t.idLease`").alias('idLease'),
            col("dmd_prin"),
            col("prin_no_vat"),
            col("prin_vat")
        )
        return dm_subquery

    def get_final_df(self, work_dfs: dict, dm_subquery: DataFrame, sub1x_subquery: DataFrame) -> DataFrame:

        final_query = work_dfs['leaseparameters'].join(
            work_dfs['keymetrics'],
            col("`k.idLease`") == col("`l.idLeaseParameters`")
        ).join(
            dm_subquery.alias('dm'),
            col('dm.idLease') == col("`l.idLeaseParameters`")
        ).join(
            sub1x_subquery.alias('sub1x'),
            col('sub1x.idLeaseParameters') == col("`l.idLeaseParameters`"),
            'left'
        )

        final_query = final_query.select(
            col("`l.idLeaseParameters`").alias('Lease ID'),
            col("`l.funder`").alias('Funder'),
            upper(col("`l.LeaseName`")).alias("Lease Name"),
            col("`l.Product`").alias('Product'),
            col("`l.status`").alias('Status'),
            spark_round(when(col("`l.Currency`") == 'MXN',
                             col("`l.DRAW`") / col("`l.FX`")).otherwise(col("`l.DRAW`")), 2).alias('DRAW'),
            spark_round(when(col("`l.Currency`") == 'MXN',
                             col("dmd_prin") / self.config_arg['fx']).otherwise(col("dmd_prin")), 2).alias('Borrowing base as of fx Date'),
            when(col("`l.Currency`") == 'MXN',
                 spark_round(col("curr_dmd_prin") / self.config_arg['fx'], 2)).otherwise(spark_round(col("curr_dmd_prin"), 2)).alias(
                'Borrowing base curr as of fx Date'),
            spark_round(when(col("`l.Currency`") == 'MXN',
                             col("dmd_prin") / col("`l.FX`")).otherwise(col("dmd_prin")), 2).alias(
                'Borrowing base as of Closing FX'),
            when(col("`l.Currency`") == 'MXN',
                 spark_round(col("curr_dmd_prin") / col("`l.FX`"), 2)).otherwise(
                spark_round(col("curr_dmd_prin"), 2)).alias('Borrowing base curr as of Closing FX'),
            upper(col("`l.Currency`")).alias('Currency'),
            when(col("`l.Currency`") == 'USD', 0).otherwise(col("`l.FX`")).alias('Closing FX'),
            col("`l.Industry`").alias('Industry'),
            col("`l.EquipmentType`").alias('Equipment Type'),
            col("`k.LTVFacilityDE`").alias('LTV'),
            col("`l.Coupon`").alias('Coupon'),
            col("`k.IRR`").alias('IRR'),
            col("`l.FirstPaymentDate`").alias('First Payment Date'),
            col("`l.Duration`").alias('Term'),
            spark_round(expr(
                "CASE WHEN months_between(current_timestamp(), `l.FirstPaymentDate`) < 0 "
                "THEN 0 ELSE months_between(current_timestamp(), `l.FirstPaymentDate`) "
                "END")).alias("Season"),
            spark_round(expr("""
            CASE WHEN (`l.Duration` - months_between(current_timestamp(), `l.FirstPaymentDate`)) < 0 
            THEN 0 ELSE (`l.Duration` - months_between(current_timestamp(), `l.FirstPaymentDate`)) 
            END""")).alias("Life"),
            col('`l.ClosingDate`').alias('Closing Date')
        ).where(
            (col('`k.deleted_at`').isNull()) &
            (col('`l.status`').isin(['ACTIVE', 'BOOKED', 'CURRENT', 'TERMINATED'])) &
            (~col('`l.status_detail`').isin(['RESTRUCTURED', 'SPECIAL CONDITIONS'])) &
            (col('`l.ClosingDate`') >= '2017-12-31') &
            (col('`l.ClosingDate`') <= '2023-11-30')

        )

        final_query.show()

        return final_query

    def main(self) -> None:

        configs: dict = ENV_VAR[self.config_arg['ENV']]
        connection_properties: dict = configs['conection_properties']

        with SSHTunnelForwarder((connection_properties['ssh_host'], connection_properties['ssh_port']),
                    ssh_username = connection_properties['ssh_port'],
                    ssh_password = connection_properties['ssh_password'],
                    remote_bind_address = (connection_properties['remote_host'], connection_properties['db_port']),
                    local_bind_address=('localhost', connection_properties['db_port'])) as tunnel:

            work_dfs: dict = {}
            borrowing_tables = BORROWING_TABLES

            for table, properties in borrowing_tables.items():
                work_dfs[table] = self.spark.read.jdbc(url=connection_properties["url"],
                                                       table=table,
                                                       properties=connection_properties)
                work_dfs[table] = work_dfs[table].select(properties['columns'])
                new_columns = [f'{properties["alias"]}.{column}' for column in properties['columns']]
                work_dfs[table] = work_dfs[table].toDF(*new_columns)

            p_subquery = self.get_subquery_p(work_dfs)

            sub1x_subquery = self.get_subquery_sub1x(p_subquery)

            dm_subquery = self.get_subquery_dm(work_dfs)

            final_query = self.get_final_df(work_dfs, dm_subquery, sub1x_subquery)

            final_query.repartition(1).write.csv('result_borrowing_base.csv', header=True, mode='overwrite')








