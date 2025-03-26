from typing import Optional, List, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col, array, lit, arrays_overlap, array_intersect, size



def contains_any(array_column: Column,values:list)->Column:

    return arrays_overlap(array_column,  array(*[lit(n) for n in values]))


def intersect_arrays(df: DataFrame, array_column: str, codes: List[str], name: str) -> DataFrame:
    """
    Intersects a column of arrays with a given set of codes and adds a boolean flag column.

    :param df: The Spark DataFrame.
    :param array_column: Column name containing array values.
    :param codes: List of codes to match.
    :param name: Prefix for the new columns.
    :return: Updated DataFrame.
    """
    return df.withColumn(f"{name}_codes",
                         array_intersect(col(array_column), array(*[lit(n) for n in codes]))) \
        .withColumn(f"{name}", size(col(f"{name}_codes")) > 0)