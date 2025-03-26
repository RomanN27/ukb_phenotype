from typing import Tuple

from pyspark.sql import DataFrame, Column
from src.phenotypes import DerivedPhenotype

from pyspark.sql import DataFrame
from src.phenotypes import ICD9DerivedPhenoType, ICD10DerivedPhenoType, VerbalInterviewDerivedPhenoType, ScoredBasedDerivedPhenoType, AnyDerivedPhenotype, DerivedPhenotype
from src.utils import contains_any
from src.phenotypes.phenotype_names import PhenotypeName
from dataclasses import dataclass, field

def self_harm_query(phenotype:DerivedPhenotype,df:DataFrame)->Tuple[DataFrame,Column]:
    # Define conditions for self-harm and suicide risk
    self_harm = (phenotype.pcol(20480) == 1) | (phenotype.pcol(20479) == 1) | contains_any(phenotype.pcol(20554), [1, 3, 4, 5, 6])

    suicide_attempt = (phenotype.pcol(20483) == 1)

    return df, self_harm | suicide_attempt




questionnaire_self_harm = DerivedPhenotype(name =PhenotypeName.QUESTIONNAIRE_SELF_HARM,
                                                   phenotype_source_field_numbers= [20480, 20483, 20479, 20554],
                                           query_callable=self_harm_query)

icd9_self_harm = ICD9DerivedPhenoType(
    name=PhenotypeName.SELF_HARM.icd9(),
    phenotype_source_codes=[
        "E950", "E951", "E952", "E953", "E954",
        "E955", "E956", "E957", "E958", "E959"
    ]
)

icd10_self_harm = ICD10DerivedPhenoType(
    name=PhenotypeName.SELF_HARM.icd10(),
    phenotype_source_codes=[
        # Intentional self-harm
        "X60", "X61", "X62", "X63", "X64", "X65", "X66", "X67", "X68", "X69",
        "X70", "X71", "X72", "X73", "X74", "X75", "X76", "X77", "X78", "X79",
        "X80", "X81", "X82", "X83", "X84",

        # Personal history of self-harm
        "Z915",

        # Y1-Y3 range (Assumed to include Y1, Y2, Y3)
        "Y1", "Y2", "Y3"
    ]
)

sr_self_harm = VerbalInterviewDerivedPhenoType(
    name=PhenotypeName.SELF_HARM.vi(),
    phenotype_source_codes=[ 1290 ]
)

self_harm = AnyDerivedPhenotype(
    name=PhenotypeName.SELF_HARM,
    derived_phenotype_sources=[
        questionnaire_self_harm,
        icd9_self_harm,
        icd10_self_harm,
        sr_self_harm
    ]
)