from pyspark.sql import DataFrame, Column

from src.phenotypes import ICD10DerivedPhenoType, VerbalInterviewDerivedPhenoType, ScoredBasedDerivedPhenoType, \
    AnyDerivedPhenotype, get_min_score_to_boolean, DerivedPhenotype
from src.phenotypes.phenotype_names import PhenotypeName






ptds_pcl6 =ScoredBasedDerivedPhenoType(name=PhenotypeName.PTSD_PCL6,
                                       phenotype_source_field_numbers=[20497, 20498, 20495, 20496, 20494, 20508], score_levels=[14 -6]) # -6 since the ukb scores 0-4 instead of 1-5 as is usual. There are 6 questions in the PCL6,  hence we subtract 6 from the sum of the scores to get the total score.



icd10_ptsd = ICD10DerivedPhenoType(
    name=PhenotypeName.PTSD.icd10(),
    phenotype_source_codes=["F431"]
)

sr_ptsd = VerbalInterviewDerivedPhenoType(
    name=PhenotypeName.PTSD.vi(),
    phenotype_source_codes=[ 1469 ]
)

ptsd  = AnyDerivedPhenotype(PhenotypeName.PTSD, derived_phenotype_sources = [icd10_ptsd, sr_ptsd, ptds_pcl6])
