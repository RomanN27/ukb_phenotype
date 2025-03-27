import json
import os
import re
from collections import defaultdict
from functools import reduce
from typing import List, Dict, Union

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import array_distinct, array_compact, array, col, concat
from pyspark.sql.functions import lit
from pyspark.sql.functions import when

from src.phenotypes import DerivedPhenotype


def load_json(file_path):
    with open(file_path, "r") as f:
        return json.load(f)


class PhenotypeQueryManager:
    def __init__(self, spark: SparkSession):

        db = os.environ.get("DB_NAME","database_gv04zbqj4qvkbzgxvpxzf6j3__app162313_20241004200320")
        spark.sql(f"USE {db}")

        self.spark = spark

        self._initialize_mappings()


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
        pattern = r"((^p[0-9]+)_i\d+)(_a\d+)?$"
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



    def query(self, *phenotypes: "DerivedPhenotype", return_only_created_columns = True) -> DataFrame:

        df = self.get_source_table(phenotypes)
        column_names_to_drop = set(df.columns) - {"eid"} if return_only_created_columns else set()

        for phenotype in phenotypes:
            if phenotype.name not in df.columns:
                df = phenotype.query(df)


        phenotype_levels = self.get_keys_at_each_level(*phenotypes)

        for level, phenotypes in phenotype_levels.items():
            phenotype_col_names = [p.name for p in phenotypes]
            df = df.withColumn(
                f'level_{level}_derived_phenotypes',
                array(*[when(col(c), lit(c)).otherwise(lit(None)).alias(c) for c in phenotype_col_names])
            )

        df  = df.drop(*column_names_to_drop)

        return df

    def get_source_table(self, phenotypes):
        field_numbers = self.get_nested_source_field_numbers(phenotypes)
        df = self.get_table(*field_numbers)
        return df
    @staticmethod
    def get_nested_source_field_numbers(phenotypes: list[DerivedPhenotype]):
        if not phenotypes:
            return set()
        field_numbers = { field_numbers for phenotype in phenotypes for field_numbers in phenotype.phenotype_source_field_numbers }
        source_phenotypes =  [  phenotype_source for phenotype in phenotypes for phenotype_source in phenotype.derived_phenotype_sources  ]
        field_numbers |= PhenotypeQueryManager.get_nested_source_field_numbers(source_phenotypes)
        return field_numbers
    @staticmethod
    def get_keys_at_each_level(d: Dict[str, Union[Dict, str]], level: int = 0,
                               result: Dict[int, List[str]] = None) -> Dict[int, List[DerivedPhenotype]]:
        if result is None:
            result = {}

        # If the current level doesn't exist in the result, initialize it as an empty list
        if level not in result:
            result[level] = []

        # Add keys of the current dictionary to the respective level
        result[level].extend(d.keys())

        # Recurse into nested dictionaries
        for key, value in d.items():
            if isinstance(value, dict):
                PhenotypeQueryManager.get_keys_at_each_level(value, level + 1, result)

        return result





