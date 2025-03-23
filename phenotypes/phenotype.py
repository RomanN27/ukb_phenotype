from abc import ABC, abstractmethod
from typing import Optional, Type

import pyspark
import pandas as pd
import json
from dataclasses import dataclass
import os
import re
from pyspark.sql.functions import (
    array, array_compact, array_distinct, lit, array_intersect,
    col, size, when
)


from typing import List, Type, Callable, Optional
from pyspark.sql import DataFrame, functions as F, Column
from pyspark.sql.functions import col, array, array_intersect, lit, size

from itertools import combinations
from collections import defaultdict
from functools import reduce

# Initialize Spark
sc = pyspark.SparkContext()
spark = pyspark.sql.SparkSession(sc)

# Load JSON mapping files
def load_json(file_path):
    with open(file_path, "r") as f:
        return json.load(f)


field_names_to_table_names:dict[str,str] = load_json("field_names_to_table_names.json")
table_names_to_field_names:dict[str, list[str]] = load_json("table_names_to_field_names.json")

table_names_to_field_numbers = {key:{int(f.split("_")[0][1:]) for f in value if f != "eid" and "hierarchy" not in f} for key, value in table_names_to_field_names.items()}

field_number_to_field_names = defaultdict(set)
for key,value in field_names_to_table_names.items():
    if not (key != "eid" and "hierarchy" not in key):
        continue
    field_number = int(key.split("_")[0][1:])
    field_number_to_field_names[field_number].add(key)

# Database selection
db = "database_gv04zbqj4qvkbzgxvpxzf6j3__app162313_20241004200320"
spark.sql(f"USE {db}")

field_number_to_table_names = defaultdict(set)
for key, value in field_names_to_table_names.items():
    if not (key != "eid" and "hierarchy" not in key):
        continue
    field_number = int(key.split("_")[0][1:])
    field_number_to_table_names[field_number].add(value)

def get_table_names_set_that_contain_field_numbers(*field_numbers:int)->set[str]:
    tables = set.union(*[field_number_to_table_names[f] for f in field_numbers])
    return tables

# Helper functions
def get_table(*field_numbers:int)->DataFrame:
    table_names = get_table_names_set_that_contain_field_numbers(*field_numbers)
    spark_tables = []

    for table_name in table_names:
        table_specific_field_names = get_table_specific_field_names(field_numbers, table_name)
        spark_table  = spark.table(table_name).select(list(table_specific_field_names))

        spark_table = handle_array_fields(spark_table, table_specific_field_names)
        spark_tables.append(spark_table)

    return reduce(lambda df1, df2: df1.join(df2, on="eid", how="outer"), spark_tables) if spark_tables else None


def handle_array_fields(spark_table, table_specific_field_names):
    pattern = r"((^[a-zA-Z]+)_i\d+)_a\d+$"
    array_fields = [field for field in table_specific_field_names if re.match(pattern, field)]
    final_field_names_to_keep = set(table_specific_field_names)
    field_instance_to_array = defaultdict(list)
    field_to_instance = defaultdict(list)
    for field in array_fields:
        match = re.match(pattern, field)
        field_instance = match.group(1)
        field_instance_to_array[field_instance].append(field)

        field_name = match.group(2)
        field_to_instance[field_name].append(field_instance)

        final_field_names_to_keep.remove(field)

        final_field_names_to_keep.add(field_instance)
        final_field_names_to_keep.add(field_name)
    for field_instance, field_instance_array in field_instance_to_array.items():
        spark_table = spark_table.withColumn(field_instance,
                                             array_distinct(array_compact(array(*field_instance_array))))
    for field_name, field_instances in field_to_instance.items():
        spark_table = spark_table.withColumn(field_name, array_distinct(array_compact(array(*field_instances))))
    return spark_table


def get_table_specific_field_names(field_numbers, table):
    relevant_field_numbers = table_names_to_field_numbers[table] & set(field_numbers)
    relevant_fields = [field_number_to_field_names[field_number] for field_number in relevant_field_numbers]
    relevant_fields = set([x for y in relevant_fields for x in y] + ["eid"])
    relevant_fields &= table_names_to_field_names[table]
    return relevant_fields

ICD_10 = 41270
ICD_9 = 41271
SR_20002 = 20002
EVER_DIAG = 20544

mother_df = get_table(ICD_10, ICD_9, SR_20002, EVER_DIAG)

def p(field_number:int,instance_number:Optional[int]=None, array_number: Optional[int]=None)->str:
    field_name = f"p{field_number}"
    if instance_number is not None:
        field_name += f"_i{instance_number}"
        if array_number is not None:
            field_name += f"_a{array_number}"

    return field_name

def pcol(field_number:int,instance_number:Optional[int]=None, array_number: Optional[int]=None)->Column:
    return col(p(field_number,instance_number,array_number))

class PhenotypeQueryStrategy:
    """ Strategy class for querying phenotype data in Spark """

    def query(self, df: DataFrame, phenotype: "PhenoType") -> DataFrame:
        """ Override this method to define custom query logic """
        raise NotImplementedError("Subclasses must implement query()")


class DefaultQueryStrategy(PhenotypeQueryStrategy):
    """ Default strategy: Just return the DataFrame as-is """

    def query(self, df: DataFrame, phenotype: "PhenoType") -> DataFrame:
        return df  # No transformation by default

class ScoreBasedQueryStrategy(PhenotypeQueryStrategy):
    """ Implements query logic based on scoring rules. """

    def __init__(self, field_numbers: List[int], score_column: str, risk_column: str,
                 score_levels: List[int], score_level_names: List[str]):
        """
        :param field_numbers: List of field numbers used for scoring.
        :param score_column: Name of the score column.
        :param risk_column: Name of the risk level column.
        :param score_levels: List of numerical thresholds for score levels.
        :param score_level_names: Corresponding names for the score levels.
        """
        self.field_numbers = field_numbers
        self.score_column = score_column
        self.risk_column = risk_column
        self.score_levels = score_levels
        self.score_level_names = score_level_names

    def query(self, df: DataFrame,phenotype:"PhenoType") -> DataFrame:
        """
        Compute scores and assign risk levels.

        :param df: The input Spark DataFrame.
        :return: DataFrame with score and risk classification.
        """
        # Replace missing values (-818) with 0
        df = df.replace(-818, 0)

        # Compute the total score
        df = df.withColumn(self.score_column, sum([pcol(x) for x in self.field_numbers]))

        # Define boundaries for risk classification
        boundaries = [(float("-inf"), self.score_levels[0])] + \
                     [(self.score_levels[i], self.score_levels[i + 1]) for i in range(len(self.score_levels) - 1)] + \
                     [(self.score_levels[-1], float("inf"))]

        # Assign risk levels using a cascading condition
        risk_expr = when(col(self.score_column).isNotNull(), None)  # Default case
        for (min_val, max_val), label in zip(boundaries, self.score_level_names):
            risk_expr = risk_expr.when((col(self.score_column) >= min_val) & (col(self.score_column) < max_val), label)

        # Apply risk classification and filter out null scores
        df = df.withColumn(self.risk_column, risk_expr).where(col(self.score_column).isNotNull())

        return df

def compose_query_strategies(*strategies: PhenotypeQueryStrategy)->PhenotypeQueryStrategy:
    """ Compose multiple query strategies into a single strategy. """
    class CompositeQueryStrategy(PhenotypeQueryStrategy):
        def query(self, df: DataFrame, phenotype: "PhenoType") -> DataFrame:
            for strategy in strategies:
                df = strategy.query(df, phenotype)
            return df

    return CompositeQueryStrategy()


class PhenoType:
    """ A phenotype representation with query handling via a strategy pattern. """

    def __init__(self,
                 name: str,
                 icd9_codes: Optional[List[str]] = None,
                 icd10_codes: Optional[List[str]] = None,
                 sr_codes: Optional[List[str]] = None,
                 ever_diag_codes: Optional[List[str]] = None,
                 associated_field_numbers: Optional[List[int]] = None,
                 query_strategy: Optional[PhenotypeQueryStrategy] = None):
        """
        Initialize a phenotype with its associated diagnostic codes and query strategy.

        :param name: Phenotype name.
        :param icd9_codes: List of ICD-9 codes.
        :param icd10_codes: List of ICD-10 codes.
        :param sr_codes: List of self-reported codes.
        :param ever_diag_codes: List of ever-diagnosed codes.
        :param associated_field_numbers: Fields used to determine phenotype.
        :param query_strategy: Strategy object handling query logic.
        """
        self.name = name
        self.icd9_codes = icd9_codes or []
        self.icd10_codes = icd10_codes or []
        self.sr_codes = sr_codes or []
        self.ever_diag_codes = ever_diag_codes or []
        self.associated_field_numbers = associated_field_numbers or []
        self.query_strategy = query_strategy or DefaultQueryStrategy()  # Default strategy

    def query(self, df: DataFrame) -> DataFrame:
        """ Execute the query strategy on the given DataFrame """
        return self.query_strategy.query(df, self)

    @staticmethod
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

    @staticmethod
    def merge_phenotypes(name: str, *phenotypes: "PhenoType") -> "PhenoType":
        """
        Merges multiple phenotypes into a new one.

        :param name: Name of the merged phenotype.
        :param phenotypes: Phenotypes to merge.
        :return: A new PhenoType instance with merged codes.
        """

        class CompositeQueryStrategy(PhenotypeQueryStrategy):
            def query(self, df: DataFrame, phenotype: "PhenoType") -> DataFrame:
                for phenotype in phenotypes:
                    df = phenotype.query(df)
                return df

        return PhenoType(
            name=name,
            icd9_codes=sum([p.icd9_codes for p in phenotypes], []),
            icd10_codes=sum([p.icd10_codes for p in phenotypes], []),
            sr_codes=sum([p.sr_codes for p in phenotypes], []),
            ever_diag_codes=sum([p.ever_diag_codes for p in phenotypes], []),
            associated_field_numbers=sum([p.associated_field_numbers for p in phenotypes], []),
            query_strategy=CompositeQueryStrategy()
        )

    def add_to_mother_df(self, mother_df: DataFrame) -> DataFrame:
        """
        Merges the phenotype's information into the global DataFrame.

        :param mother_df: The global Spark DataFrame.
        :param get_table: Function to retrieve associated fields.
        :param p: Column name resolver function.
        :return: Updated DataFrame with phenotype indicators.
        """
        associated_df = get_table(*self.associated_field_numbers)
        associated_df = self.query(associated_df)
        mother_df = mother_df.join(associated_df, on="eid", how="outer")
        if self.icd9_codes:
            mother_df = self.intersect_arrays(mother_df, p(ICD_9), self.icd9_codes, "icd9_" + self.name)
        if self.icd10_codes:
            mother_df = self.intersect_arrays(mother_df, p(ICD_10), self.icd10_codes, "icd10_" + self.name)
        if self.sr_codes:
            mother_df = self.intersect_arrays(mother_df, p(SR_20002), self.sr_codes, "sr_20002_" + self.name)

            for i in range(4):
                mother_df = self.intersect_arrays(mother_df, p(SR_20002, instance_number=i), self.sr_codes,
                                                  f"sr_20002_{i}_" + self.name)
        if self.ever_diag_codes:

            mother_df = self.intersect_arrays(mother_df, p(EVER_DIAG), self.ever_diag_codes, "ever_diag_" + self.name)

        return mother_df

