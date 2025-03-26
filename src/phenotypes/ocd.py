from src.phenotypes import ICD9DerivedPhenoType, ICD10DerivedPhenoType, VerbalInterviewDerivedPhenoType, EverDiagnosedDerivedPhenoType, AnyDerivedPhenotype
from src.phenotypes.phenotype_names import PhenotypeName

icd9_ocd = ICD9DerivedPhenoType(
    name=PhenotypeName.OCD.icd9(),
    phenotype_source_codes=["3003"]
)

icd10_ocd = ICD10DerivedPhenoType(
    name=PhenotypeName.OCD.icd10(),
    phenotype_source_codes=["F42.0", "F42.1", "F42.2", "F42.8", "F42.9"]
)

sr_ocd = VerbalInterviewDerivedPhenoType(
    name=PhenotypeName.OCD.vi(),
    phenotype_source_codes=[ 1615 ]
)

ever_diag_ocd = EverDiagnosedDerivedPhenoType(
    name=PhenotypeName.OCD.ever_diag(),
    phenotype_source_codes=[7]
)

ocd = AnyDerivedPhenotype(
    name=PhenotypeName.OCD,
    derived_phenotype_sources=[icd9_ocd, icd10_ocd, sr_ocd, ever_diag_ocd]
)
