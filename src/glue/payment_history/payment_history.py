from pyspark.sql.window import Window
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, expr, concat, datediff, sum as spark_sum, max as spark_max


class PaymentHistory:

    def __init__(self, spark: SparkSession, config_arg: dict):
        self.spark = spark
        self.config_arg = config_arg

    def get_aurora_table(self, table_name: str) -> DataFrame:
        ## Verificacion Pendiente

        endpoint = self.config_arg["endpoint"]
        port = self.config_arg["port"]

        table = self.spark.read.jdbc(f'jdbc:mysql://{endpoint}:{port}/{table_name}')
        print(f'Tabla: {table_name}')
        table.show()

        return table

    @staticmethod
    def specific_column_filter(df: DataFrame, columns: list) -> DataFrame:
        """
        Filters out the specified columns from the df
        :param df: Origin df
        :param columns: To use columns
        :return:
        """
        return df.select("', '".join(columns))

    def generate_payment_history(self) -> DataFrame:


        work_dfs: dict = {}

        for table, columns in self.config_arg['tables'].items():
            work_dfs[table] = self.get_aurora_table(table)
            work_dfs[table] = self.specific_column_filter(work_dfs[table], columns)

        window_spec = Window.partitionBy("LeaseName", "dDate")

        result_df = (
            work_dfs['totalleasecf'].alias("t")
            .join(work_dfs['leaseparameters'].alias("l"), col("l.idLeaseParameters") == col("t.idLease"))
            .join(work_dfs['vat'].alias("v"),
                  (col("v.idCF") == col("t.idTotalLeaseCF")) & (col("v.idLease") == col("t.idLease")), "left")
            .join(work_dfs['invoice'].alias("i"),
                  (col("t.idTotalLeaseCF") == col("i.cashflow_id")) & (col("t.idLease") == col("i.lease_id")) & (
                      col("i.status_id").isin([1, 3, 4])), "left")
            .join(work_dfs['payment'].alias("p"), (col("p.invoice_id") == col("i.id")) & (~col("p.status_id").isin([2])), "left")
            .filter((col("t.re_sync_status") == "Active") & (
                col("t.idLease").isin([440])))  # Modifica el valor de 440 si es necesario
            .groupby(
                "l.LeaseName", "i.invoice_number", "l.status_detail", "l.Product", "i.invoice_date", "l.Currency",
                "t.dDate", "t.dMonth", "t.PPMT", "t.IPMT", "t.PMT", "t.is_residualvalue", "v.NetVAT", "v.VATKept",
                expr("(t.ResidualVal + t.BuyOut) AS RV"),
                "t.is_initial", "i.odoo_invoice_id", "t.idTotalLeaseCF"
            )
            .agg(
                spark_max("p.payment_date").alias("payment_date"),
                expr(
                    "IF(DATEDIFF(max(p.payment_date),t.dDate) > 30, CONCAT('PAR ',DATEDIFF(max(p.payment_date),t.dDate)),DATEDIFF(max(p.payment_date),t.dDate))").alias(
                    "days_late"),
                spark_sum("p.amount").alias("payment_amount"),
                spark_sum("p.amount_vat").alias("payment_vat")
            )
            .withColumn("net_payment", col("payment_amount") - col("payment_vat"))
            .orderBy("l.LeaseName", "t.dDate")
        )

        result_df.show()

        return result_df

    def main(self) -> None:

        payment_df = self.get_aurora_table('payment')

        ## Exportacion a CSV y a Excel
        ## Ya que se puede realizar en una linea
        ## Si es necesario que se realice
        ## customizacion se deberá generar una funcion especifica

