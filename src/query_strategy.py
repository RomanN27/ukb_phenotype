from pyspark.sql.functions import when

from src.phenotypes import PhenoType
from src.utils import pcol, p
from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.functions import array_intersect, lit, size

import json
import re
from collections import defaultdict
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import array_distinct, array_compact, array, col, concat

ICD_10 = 41270
ICD_9 = 41271
SR_20002 = 20002
EVER_DIAG = 20544

DIAGNOSIS_FIELDS = [ICD_10, ICD_9, SR_20002, EVER_DIAG]


def load_json(file_path):
    with open(file_path, "r") as f:
        return json.load(f)


class PhenotypeQueryManager:
    def __init__(self, spark: SparkSession):

        db = "database_gv04zbqj4qvkbzgxvpxzf6j3__app162313_20241004200320"
        spark.sql(f"USE {db}")

        self.spark = spark

        self._initialize_mappings()

        self.df = self.get_table(ICD_10, ICD_9, SR_20002, EVER_DIAG)

    def _initialize_mappings(self):
        self.field_names_to_table_names: dict[str, str] = load_json("field_names_to_table_names.json")
        self.table_names_to_field_names: dict[str, set[str]] = load_json("table_names_to_field_names.json")
        self.table_names_to_field_names = {key: set(value) for key, value in self.table_names_to_field_names.items()}
        self.table_names_to_field_numbers: dict[str, set[int]] = self._get_table_names_to_field_numbers(
            self.table_names_to_field_names)
        self.field_number_to_field_names: dict[int, set[str]] = self._get_field_number_to_field_names()
        self.field_number_to_table_names: dict[int, set[str]] = self._get_field_number_to_table_names()

    def _get_field_number_to_table_names(self):
        field_number_to_table_names = defaultdict(set)
        for key, value in self.field_names_to_table_names.items():
            if not (key != "eid" and "hierarchy" not in key):
                continue
            field_number = int(key.split("_")[0][1:])
            field_number_to_table_names[field_number].add(value)

        return field_number_to_table_names

    def get_table_names_set_that_contain_field_numbers(self, *field_numbers: int) -> set[str]:
        tables = set.union(*[self.field_number_to_table_names[f] for f in field_numbers])
        return tables

    def get_table(self, *field_numbers: int) -> DataFrame:
        table_names = self.get_table_names_set_that_contain_field_numbers(*field_numbers)
        spark_tables = []

        for table_name in table_names:
            table_specific_field_names = self.get_table_specific_field_names(field_numbers, table_name)
            spark_table = self.spark.table(table_name).select(list(table_specific_field_names))

            spark_table = self.handle_array_fields(spark_table, table_specific_field_names)
            spark_tables.append(spark_table)

        return reduce(lambda df1, df2: df1.join(df2, on="eid", how="outer"), spark_tables) if spark_tables else None

    def handle_array_fields(self, spark_table, table_specific_field_names):
        pattern = r"((^p[0-9]+)_i\d+)_a\d+$"
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
            spark_table = spark_table.withColumn(field_name, array_distinct(concat(*field_instances)))
        return spark_table

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

    def get_table_specific_field_names(self, field_numbers, table):
        relevant_field_numbers = self.table_names_to_field_numbers[table] & set(field_numbers)
        relevant_fields = [self.field_number_to_field_names[field_number] for field_number in relevant_field_numbers]
        relevant_fields = set([x for y in relevant_fields for x in y] + ["eid"])
        relevant_fields &= self.table_names_to_field_names[table]
        return relevant_fields

    def _get_field_number_to_field_names(self):
        field_number_to_field_names = defaultdict(set)
        for field_name in list(self.field_names_to_table_names):
            if not (field_name != "eid" and "hierarchy" not in field_name):
                continue
            field_number = int(field_name.split("_")[0][1:])
            field_number_to_field_names[field_number].add(field_name)

        return field_number_to_field_names

    def _get_table_names_to_field_numbers(self, table_names_to_field_names):
        return {
            key: {int(f.split("_")[0][1:]) for f in value if f != "eid" and "hierarchy" not in f} for key, value in
            table_names_to_field_names.items()}

    def query_diagnoses(self, df: DataFrame, phenotype: "PhenoType") -> DataFrame:
        diagnoses_names = []

        def add_diagnosis(df, codes, field, name,add_diagnosis=True ,cast_to_int=False):
            if codes:
                if cast_to_int:
                    codes = [int(x) for x in codes]
                df = self.intersect_arrays(df, field, codes, name)
                if add_diagnosis:
                    diagnoses_names.append(name)
            return df

        df = add_diagnosis(df, phenotype.icd9_codes, p(ICD_9), "icd9_" + phenotype.name)
        df = add_diagnosis(df, phenotype.icd10_codes, p(ICD_10), "icd10_" + phenotype.name)
        df = add_diagnosis(df, phenotype.sr_codes, p(SR_20002), "sr_20002_" + phenotype.name, cast_to_int=True)

        for i in range(4):
            df = add_diagnosis(df, phenotype.sr_codes , p(SR_20002, i), f"sr_20002_{i}_{phenotype.name}",add_diagnosis=False, cast_to_int=True)

        df = add_diagnosis(df, phenotype.ever_diag_codes, EVER_DIAG, "ever_diag_" + phenotype.name)

        if not diagnoses_names:
            return df

        any_diagnosis = reduce(lambda x, y: x | y, [col(name) for name in diagnoses_names])
        df = df.withColumn(phenotype.name, any_diagnosis)

        return df

    def query(self, df: DataFrame, phenotype: "PhenoType") -> DataFrame:
        if phenotype.associated_field_numbers:
            new_df = self.get_table(*phenotype.associated_field_numbers)
            df = df.join(new_df, on="eid", how="outer")

        if phenotype.query:
            df = phenotype.query(df)
        df = self.query_diagnoses(df, phenotype)
        return df

    def query_all(self, *phenotypes: "PhenoType") -> DataFrame:

        df = self.get_relevant_data_frame(*phenotypes)

        for phenotype in phenotypes:
            if phenotype.query:
                df = phenotype.query(df)
                
            df = self.query_diagnoses(df, phenotype)
            
        return df

    def get_relevant_data_frame(self, *phenotypes: "PhenoType") -> DataFrame:
        df = self.df
        # remove duplicate fieldnumbers since this causes issues with spark
        unique_field_numbers = set()
        for phenotype in phenotypes:
            unique_field_numbers |= set(phenotype.associated_field_numbers)
        unique_field_numbers -= set(DIAGNOSIS_FIELDS)
        if unique_field_numbers:
            new_df = self.get_table(*unique_field_numbers)
            df = df.join(new_df, on="eid", how="outer")
        return df


class ScoreBasedQueryStrategy:
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

    def __call__(self, df: DataFrame) -> DataFrame:
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

