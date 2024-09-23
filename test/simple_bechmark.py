from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import matplotlib.pylab as plt
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit


def bad_spark_function(n: int, spark: SparkSession, test_data: DataFrame) -> None:
    for i in range(n):
        test_data = test_data.withColumn(f"col_{i}", lit(i))

    test_data.write.mode("overwrite").format("noop").save()


def good_spark_function(n: int, spark: SparkSession, test_data: DataFrame) -> None:
    test_data = test_data.withColumns({f"col_{i}": lit(i) for i in range(n)})
    test_data.write.mode("overwrite").format("noop").save()


def wrap(
    n: int,
    spark_session: SparkSession,
    tdf: DataFrame,
    fun: Callable[[int, SparkSession, DataFrame], None],
) -> float:
    start_t = datetime.now()
    fun(n, spark_session, tdf)
    end_t = datetime.now()

    return (end_t - start_t).total_seconds()


if __name__ == "__main__":
    spark = SparkSession.builder.master("local[1]").getOrCreate()
    iris = spark.createDataFrame(
        pd.read_csv(
            "https://raw.githubusercontent.com/mwaskom/seaborn-data/refs/heads/master/iris.csv"
        )
    )
    iris.write.mode("overwrite").parquet("tmp.parquet")

    test_data = spark.read.parquet("tmp.parquet").persist()
    test_data.count()

    results = {
        "n_cols": [],
        "withColumn": [],
        "withColumns": [],
    }
    print_row = "n: {}\tbad: {}\tgood: {}"

    for n in range(0, 1000, 100):
        bad_res = wrap(n, spark, test_data, bad_spark_function)
        good_res = wrap(n, spark, test_data, good_spark_function)

        results["n_cols"].append(n)
        results["withColumn"].append(bad_res)
        results["withColumns"].append(good_res)
        print(print_row.format(n, bad_res, good_res))

    f = plt.figure(figsize=(6, 4))
    ax = f.add_subplot()
    ax.plot(
        results["n_cols"],
        results["withColumn"],
        color="red",
        label="withColumn",
    )
    ax.plot(
        results["n_cols"],
        results["withColumns"],
        color="blue",
        label="withColumns",
    )
    ax.set_ylabel("Time, seconds")
    ax.set_xlabel("An amount of columns to add")
    ax.legend()
    f.tight_layout()

    static_root = Path(__file__).parent.parent.joinpath("static")
    static_root.mkdir(parents=True, exist_ok=True)
    f.savefig(
        str(static_root.joinpath("with_column_performance.png").absolute()),
        dpi=100,
    )
