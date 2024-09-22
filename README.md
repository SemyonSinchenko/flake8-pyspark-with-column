# Flake8-pyspark-with-column

A flake8 plugin that detects of usage `withColumn` in a loop or inside `reduce`. From the PySpark documentation about `withColumn` method:

```
  This method introduces a projection internally.
  Therefore, calling it multiple times, for instance, via loops in order to add multiple columns
  can generate big plans which can cause performance issues and even StackOverflowException.
  To avoid this, use select() with multiple columns at once.
```

## Rules
This plugin contains the following rules:

- `PSPRK001`: Usage of withColumn in a loop detected
- `PSPRK002`: Usage of withColumn iside reduce is detected
