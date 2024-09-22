from functools import reduce

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    data = spark.createDataFrame(
        [
            ["foo", 1, 4],
            ["bar", 2, 6],
            ["baz", 3, None],
        ],
        schema="struct<a:string, b:int, c:int>",
    )

    # Using withColumn in a loop tends to huge performance degradation
    for i in range(100):
        data = data.withColumn(f"t{i}", i * i)

    # The same problem appears if withColumn is called inside the reduce body
    data = reduce(lambda df, i: df.withColumn(f"f{i}", i * i), range(100), data)
