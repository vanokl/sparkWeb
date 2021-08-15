from pyspark.sql import SparkSession

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars file:///C:/Users/mi/PycharmProjects/sparkWeb/postgresql-42.2.23.jar pyspark-shell'

spark = SparkSession.builder \
    .master("local[*]") \
    .getOrCreate()

sourceDf = spark.read.format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("user", "postgres") \
    .option("password", "docker") \
    .option("dbtable", "test") \
    .option("fetchsize", 100000) \
    .load()
sourceDf.show()

def load(parameters):
    if parameters.src_table.upper().strip().startswith("SELECT"):
        parameters.src_table = "(" + parameters.src_table + ") t"


    spark = SparkSession.builder \
        .master("local[*]") \
        .getOrCreate()


    if parameters.src_type == "DB" and parameters.trg_type == "HIVE":
        sourceDf = spark.read.format("jdbc") \
            .option("driver", parameters.src_driver)\
            .option("url", parameters.src_url)\
            .option("user", parameters.src_user)\
            .option("password", parameters.src_password)\
            .option("query", parameters.src_table)\
            .option("fetchsize", 100000)\
            .load()

        sourceDf.write\
            .mode(parameters.overwrite_flg)\
            .saveAsTable(parameters.trg_table)

    elif parameters.src_type == "DB" and parameters.trg_type == "DB":

        sourceDf = spark.read.format("jdbc") \
            .option("driver", parameters.src_driver) \
            .option("url", parameters.src_url) \
            .option("user", parameters.src_user) \
            .option("password", parameters.src_password) \
            .option("query", parameters.src_table) \
            .option("fetchsize", 100000) \
            .load()

        sourceDf.write \
            .format("jdbc") \
            .option("driver", parameters.tgt_driver) \
            .option("url", parameters.tgt_url) \
            .option("user", parameters.tgt_user) \
            .option("password", parameters.tgt_password) \
            .option("query", parameters.tgt_table) \
            .save()

    elif parameters.src_type == "HIVE" and parameters.trg_type == "DB":

        sourceDf = spark.sql(f"select * from {parameters.src_table}")

        sourceDf.write \
            .format("jdbc") \
            .option("driver", parameters.tgt_driver) \
            .option("url", parameters.tgt_url) \
            .option("user", parameters.tgt_user) \
            .option("password", parameters.tgt_password) \
            .option("query", parameters.tgt_table) \
            .save()

    # if parameters.src_type == "DB" and parameters.trg_type == "HIVE":
    #     sourceDf = spark.read.format("jdbc")\
    #         .option("header", "true")\
    #         .load("2010-summary.csv")\
    #         .option("driver", parameters.src_driver)\
    #         .option("url", parameters.src_url)\
    #         .option("user", parameters.src_user)\
    #         .option("password", parameters.src_password)\
    #         .option("dbtable", parameters.src_table)\
    #         .option("fetchsize", 100000)\
    #         .show()
        #
        # sourceDf.write\
        #     .mode(parameters.overwrite_flg)\
        #     .saveAsTable(parameters.trg_table)
        #


