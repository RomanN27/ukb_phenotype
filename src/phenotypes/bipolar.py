from pyspark.sql import DataFrame
from pyspark.sql.functions import size, when, col
from src.phenotypes import PhenoType, depression_name
from src.utils import pcol
from functools import reduce

def probable_bipolar_query(df: DataFrame) -> DataFrame:
    instances = 4
    for i in range(instances):
        probable_bipolar_ii = when(
            (
                    (pcol(4642,i) == 1) | (pcol(4653,i) == 1)  # Condition 1: Manic/hypomanic symptoms
            ) &
            (
                    size(pcol(6156,i)) >= 3  # Condition 2: At least three symptoms
            ) &
            (
                (pcol(5663,i) == 13)  # Condition 3: Specific symptom criteria
            ), True).otherwise(False)

        df = df.withColumn(f"probable_bipolar_II_{i}", probable_bipolar_ii)

        df = df.withColumn(f"probable_bipolar_I_{i}",
                           when(col(f"probable_bipolar_II_{i}") & (pcol(5674,i) == 12), True).otherwise(False))
    
    any_probable_bipolar_ii = [col(f"probable_bipolar_II_{i}") for i in range(instances)]
    any_probable_bipolar_i = [col(f"probable_bipolar_I_{i}") for i in range(instances)]
    
    df = df.withColumn("probable_bipolar_II", reduce(lambda x, y: x | y, any_probable_bipolar_ii))
    df = df.withColumn("probable_bipolar_I", reduce(lambda x, y: x | y, any_probable_bipolar_i))
    
    df = df.withColumn("ProbableBipolar", col("probable_bipolar_I") | col("probable_bipolar_II"))
    return df

probable_bipolar = PhenoType(name="ProbableBipolar",
                         associated_field_numbers=[4642, 4653, 6156, 5663, 5674],
                             query=probable_bipolar_query)

def life_time_bipolar_query(df: DataFrame) -> DataFrame:
    bipolar_affective_disorder_ii = when(
        (col(depression_name) == 1) &  # Prior depression phenotype
        (
                (pcol(20501) == 1) | (pcol(20502) == 1)  # Mania or extreme irritability
        ) &
        (
            (size(pcol(20548)) >= 4)  # At least four symptoms
        ) &
        (
                pcol(20492) == 3  # Period of mania or irritability
        ), True).otherwise(False)

    df = df.withColumn("bipolar_affective_disorder_II", bipolar_affective_disorder_ii)

    df = df.withColumn("bipolar_affective_disorder_I",
                       when(col("bipolar_affective_disorder_II") & (pcol(20493) == 1), True).otherwise(False))
    
    df = df.withColumn("LifetimeBipolar", col("bipolar_affective_disorder_I") | col("bipolar_affective_disorder_II"))
    return df

life_time_bipolar = PhenoType(name="LifetimeBipolar",
                         associated_field_numbers=[20501, 20502, 20548, 20492, 20493],
                              query=life_time_bipolar_query)


diagnosed_bipolar = PhenoType(name="Bipolar",  icd9_codes=["2960", "2961", "2966"], icd10_codes=[
            "F30.0", "F30.1", "F30.2", "F30.8", "F30.9",  # Manic Episode
            "F31.0", "F31.1", "F31.2", "F31.3", "F31.4", "F31.5", "F31.6", "F31.7", "F31.8", "F31.9"],
                         sr_codes=["1291"], ever_diag_codes=["10"])

bipolar  = PhenoType.merge_phenotypes("Bipolar", probable_bipolar, life_time_bipolar, diagnosed_bipolar)

