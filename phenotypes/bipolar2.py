from pyspark.sql import DataFrame
from pyspark.sql.functions import size, when
from phenotype import PhenoType
from phenotypes.phenotype import pcol


class ProbableBipolar(PhenoType):
    def __init__(self):
        super().__init__(
            name="ProbableBipolar",
            associated_field_numbers=[4642, 4653, 6156, 5663, 5674],
        )

    def query(self, df: DataFrame) -> DataFrame:
        probable_bipolar_ii = when(
            (
                    (pcol(4642) == 1) | (pcol(4653) == 1)  # Condition 1: Manic/hypomanic symptoms
            ) &
            (
                    size(pcol(6156)) >= 3  # Condition 2: At least three symptoms
            ) &
            (
                (pcol(5663) == 13)  # Condition 3: Specific symptom criteria
            ), True).otherwise(False)

        df = df.withColumn("probable_bipolar_II", probable_bipolar_ii)

        df = df.withColumn("probable_bipolar_I",
                           when(col("probable_bipolar_II") & (pcol(5674) == 12), True).otherwise(False))

        return df


class LifetimeBipolar(PhenoType):
    def __init__(self):
        super().__init__(
            name="LifetimeBipolar",
            associated_field_numbers=[20501, 20502, 20548, 20492, 20493],
        )

    def query(self, df: DataFrame) -> DataFrame:
        bipolar_affective_disorder_ii = when(
            (col("depression_ever") == 1) &  # Prior depression phenotype
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

        return df


class Bipolar(PhenoType):
    def __init__(self):
        super().__init__(
            name="Bipolar",
            icd9_codes=["2960", "2961", "2966"],
            icd10_codes=[
            "F30.0", "F30.1", "F30.2", "F30.8", "F30.9",  # Manic Episode
            "F31.0", "F31.1", "F31.2", "F31.3", "F31.4", "F31.5", "F31.6", "F31.7", "F31.8", "F31.9"],
            sr_codes=["1291"],
            ever_diag_codes=["10"]
        )


GeneralBipolar = PhenoType.merge_phenotypes(
    "GeneralBipolar", ProbableBipolar(), LifetimeBipolar(), Bipolar()
)
