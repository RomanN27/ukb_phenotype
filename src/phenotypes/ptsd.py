from pyspark.sql import DataFrame, Column

from src.phenotypes import ICD10DerivedPhenoType, VerbalInterviewDerivedPhenoType, ScoredBasedDerivedPhenoType, \
    AnyDerivedPhenotype, get_min_score_to_boolean, DerivedPhenotype
from src.phenotypes.phenotype_names import PhenotypeName



def pcl6_scorer(phenotype:ScoredBasedDerivedPhenoType)->Column:
        pcl6_score = (
                (phenotype.pcol(20497) - 1) +
                (phenotype.pcol(20498) - 1) +
                (phenotype.pcol(20495) - 1) +
                (phenotype.pcol(20496) - 1) +
                (phenotype.pcol(20494) - 1) +
                (phenotype.pcol(20508) - 1))
        return pcl6_score


ptds_pcl6 =ScoredBasedDerivedPhenoType(name=PhenotypeName.PTSD_PCL6, phenotype_source_field_numbers=[20497, 20498, 20495, 20496, 20494, 20508], score_levels=[14],make_score_column=pcl6_scorer)



icd10_ptsd = ICD10DerivedPhenoType(
    name=PhenotypeName.PTSD.icd10(),
    phenotype_source_codes=["F431"]
)

sr_ptsd = VerbalInterviewDerivedPhenoType(
    name=PhenotypeName.PTSD.vi(),
    phenotype_source_codes=["1469"]
)

ptsd  = AnyDerivedPhenotype(PhenotypeName.PTSD, derived_phenotype_sources = [icd10_ptsd, sr_ptsd, ptds_pcl6])
