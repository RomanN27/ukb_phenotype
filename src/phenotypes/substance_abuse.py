from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import when, array

from src.phenotypes import DerivedPhenotype
from src.utils import pcol, contains_any


def substance_abuse_non_alcoholic_query(df: DataFrame) -> DataFrame:
    # Define cannabis use groups
    cannabis_use_group = when(pcol(20453) == 0, 0) \
        .when((pcol(20453) == 1) | (pcol(20453) == 2), 1) \
        .when(pcol(20453) == 3, 2) \
        .when((pcol(20453) == 4) & (pcol(20454) == 1), 3) \
        .when((pcol(20453) == 4) & (pcol(20454) == 2), 4) \
        .when((pcol(20453) == 4) & (pcol(20454) == 3), 5) \
        .when((pcol(20453) == 4) & (pcol(20454) == 4), 6) \
        .otherwise(None)

    df = df.withColumn("cannabis_use_group", cannabis_use_group)

    # Define substance addiction criteria
    cannabis_dependency = col("cannabis_use_group") == 6
    illness_dependency = contains_any(pcol(20002), [785,48,53])
    illicit_drug_addiction = pcol(20456) == 1
    medication_addiction = pcol(20503) == 1

    substance_addiction = illness_dependency | illicit_drug_addiction | medication_addiction | cannabis_dependency

    df = df.withColumn("SubstanceAbuseNonAlcoholic", substance_addiction)

    return df

substance_abuse_non_alcoholic = DerivedPhenotype(name="SubstanceAbuseNonAlcoholic", icd9_codes=["3040"],
                                                 icd10_codes=[
                             # Mental and behavioural disorders due to use of opioids
                             "F11.1", "F11.2", "F11.3", "F11.4",

                             # Mental and behavioural disorders due to use of cannabinoids
                             "F12.1", "F12.2", "F12.3", "F12.4",

                             # Mental and behavioural disorders due to use of sedatives or hypnotics
                             "F13.1", "F13.2", "F13.3", "F13.4",

                             # Mental and behavioural disorders due to use of cocaine
                             "F14.1", "F14.2", "F14.3", "F14.4",

                             # Mental and behavioural disorders due to multiple drug use and other psychoactive substances
                             "F19.1", "F19.2", "F19.3", "F19.4"
                         ], sr_codes=["1409", "1410"], associated_field_numbers=[20453, 20454, 20002, 20456, 20503], query=substance_abuse_non_alcoholic_query)


def substance_abuse_alcoholic_query(df: DataFrame) -> DataFrame:
    # Calculate AUDIT score
    adjusted_scores = {
        "p20414": pcol(20414) - 1,
        "p20403": pcol(20403) - 1,
        "p20416": pcol(20416) - 1,
        "p20413": pcol(20413) - 1,
        "p20407": pcol(20407) - 1,
        "p20412": pcol(20412) - 1,
        "p20409": pcol(20409) - 1,
        "p20408": pcol(20408) - 1,
        "p20411": when(pcol(20411) == 2, 4).when(pcol(20411) == 1, 2).otherwise(0),
        "p20405": when(pcol(20405) == 2, 4).when(pcol(20405) == 1, 2).otherwise(0)
    }

    alcohol_addiction = pcol(20406) == 1
    df = df.withColumn("AUDIT_score", sum(adjusted_scores.values()))
    df = df.withColumn("AlcoholAbuse", alcohol_addiction | (col("AUDIT_score") >= 15))
    return df

alcohol_abuse = DerivedPhenotype(name="AlcoholAbuse", icd9_codes=["3039"],
                                 icd10_codes=["F10.1", "F10.2", "F10.3", "F10.4"], sr_codes=["1408"],
                                 associated_field_numbers=[20414, 20403, 20416, 20413, 20407, 20412, 20409, 20408, 20411, 20405,
                                                   20406], query=substance_abuse_alcoholic_query)

