from typing import Tuple

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col
from pyspark.sql.functions import when, array

from src.phenotypes import DerivedPhenotype
from src.utils import contains_any
from src.phenotypes import DerivedPhenotype


from src.phenotypes import ICD9DerivedPhenoType, ICD10DerivedPhenoType, VerbalInterviewDerivedPhenoType, EverDiagnosedDerivedPhenoType, AnyDerivedPhenotype, ScoredBasedDerivedPhenoType,OneBooleanFieldPhenotype
from src.phenotypes.phenotype_names import PhenotypeName


def substance_abuse_non_alcoholic_query(phenotype: DerivedPhenotype, df: DataFrame) -> Tuple[DataFrame, Column]:
    # Define cannabis use groups
    cannabis_use_group = when(phenotype.pcol(20453) == 0, 0) \
        .when((phenotype.pcol(20453) == 1) | (phenotype.pcol(20453) == 2), 1) \
        .when(phenotype.pcol(20453) == 3, 2) \
        .when((phenotype.pcol(20453) == 4) & (phenotype.pcol(20454) == 1), 3) \
        .when((phenotype.pcol(20453) == 4) & (phenotype.pcol(20454) == 2), 4) \
        .when((phenotype.pcol(20453) == 4) & (phenotype.pcol(20454) == 3), 5) \
        .when((phenotype.pcol(20453) == 4) & (phenotype.pcol(20454) == 4), 6) \
        .otherwise(None)

    df = df.withColumn("cannabis_use_group", cannabis_use_group)

    # Define substance addiction criteria
    cannabis_dependency = col("cannabis_use_group") == 6
    illness_dependency = contains_any(phenotype.pcol(20002), [785, 48, 53])
    illicit_drug_addiction = phenotype.pcol(20456) == 1
    medication_addiction = phenotype.pcol(20503) == 1

    substance_addiction = illness_dependency | illicit_drug_addiction | medication_addiction | cannabis_dependency

    return df, substance_addiction



questionnaire_substance_abuse_non_alcoholic = DerivedPhenotype(name = PhenotypeName.QUESTIONNAIRE_SUBSTANCE_ABUSE_NON_ALCOHOLIC,
                                                                                         phenotype_source_field_numbers= [20453, 20454, 20002, 20456, 20503],
                                                               query_callable=substance_abuse_non_alcoholic_query)



icd10_substance_abuse = ICD10DerivedPhenoType(PhenotypeName.SUBSTANCE_ABUSE_NON_ALCOHOLIC.icd10(), phenotype_source_codes=[
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
                         ])
icd9_substance_abuse = ICD9DerivedPhenoType(PhenotypeName.SUBSTANCE_ABUSE_NON_ALCOHOLIC.icd9(), phenotype_source_codes=["3040"])
sr_substance_abuse = VerbalInterviewDerivedPhenoType(PhenotypeName.SUBSTANCE_ABUSE_NON_ALCOHOLIC.vi(), phenotype_source_codes=["1409", "1410"])
ever_diag_substance_abuse = EverDiagnosedDerivedPhenoType(PhenotypeName.SUBSTANCE_ABUSE_NON_ALCOHOLIC.ever_diag(), phenotype_source_codes=[8])

substance_abuse_non_alcoholic = AnyDerivedPhenotype(name = PhenotypeName.SUBSTANCE_ABUSE_NON_ALCOHOLIC, derived_phenotype_sources = [questionnaire_substance_abuse_non_alcoholic, icd10_substance_abuse, icd9_substance_abuse, sr_substance_abuse, ever_diag_substance_abuse])


def audit_scorer(phenotype:DerivedPhenotype) -> Column:
    adjusted_scores = {
        "p20414": phenotype.pcol(20414) - 1,
        "p20403": phenotype.pcol(20403) - 1,
        "p20416": phenotype.pcol(20416) - 1,
        "p20413": phenotype.pcol(20413) - 1,
        "p20407": phenotype.pcol(20407) - 1,
        "p20412": phenotype.pcol(20412) - 1,
        "p20409": phenotype.pcol(20409) - 1,
        "p20408": phenotype.pcol(20408) - 1,
        "p20411": when(phenotype.pcol(20411) == 2, 4).when(phenotype.pcol(20411) == 1, 2).otherwise(0),
        "p20405": when(phenotype.pcol(20405) == 2, 4).when(phenotype.pcol(20405) == 1, 2).otherwise(0)
    }

    score_column = sum(adjusted_scores.values())

    return score_column



audit_score_alcohol_addiction = ScoredBasedDerivedPhenoType(name = PhenotypeName.AUDIT_SCORE, phenotype_source_field_numbers= [20414, 20403, 20416, 20413, 20407, 20412, 20409, 20408, 20411, 20405], score_levels=[15],
                                                            make_score_column=audit_scorer)
alcohol_addiction = OneBooleanFieldPhenotype(name = PhenotypeName.ALCOHOL_ADDICTION, source_field_number = 20406)
alcohol_abuse = AnyDerivedPhenotype(name = PhenotypeName.ALCOHOL_ABUSE, derived_phenotype_sources = [audit_score_alcohol_addiction, alcohol_addiction])

