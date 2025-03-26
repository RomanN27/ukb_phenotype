from src.phenotypes import ICD9DerivedPhenoType, ICD10DerivedPhenoType, VerbalInterviewDerivedPhenoType, EverDiagnosedDerivedPhenoType, ScoredBasedDerivedPhenoType, AnyDerivedPhenotype
from src.phenotypes.phenotype_names import PhenotypeName

gad7_anxiety = ScoredBasedDerivedPhenoType(
    name=PhenotypeName.ANXIETY_GAD7,
    phenotype_source_field_numbers=[20506, 20509, 20520, 20515, 20516, 20505, 20512],
    score_levels=[5, 10, 15],
    severity_names=["Low", "Mild", "Moderate", "Severe"]
)

icd9_anxiety = ICD9DerivedPhenoType(
    name=PhenotypeName.ANXIETY.icd9(),
    phenotype_source_codes=["3000", "3001", "3002", "3009", "300"]
)

icd10_anxiety = ICD10DerivedPhenoType(
    name=PhenotypeName.ANXIETY.icd10(),
    phenotype_source_codes=[
        "F064", "F93.0", "F931", "F932",
        "F40", "F400", "F401", "F402", "F408", "F409",
        "F41", "F410", "F411", "F412", "F413", "F418", "F419"
    ]
)

sr_anxiety = VerbalInterviewDerivedPhenoType(
    name=PhenotypeName.ANXIETY.vi(),
    phenotype_source_codes=[ 1287 ,  1614 ]
)

ever_diag_anxiety = EverDiagnosedDerivedPhenoType(
    name=PhenotypeName.ANXIETY.ever_diag(),
    phenotype_source_codes=[1, 6, 17]
)

anxiety = AnyDerivedPhenotype(
    name=PhenotypeName.ANXIETY,
    derived_phenotype_sources=[
        gad7_anxiety,
        icd9_anxiety,
        icd10_anxiety,
        sr_anxiety,
        ever_diag_anxiety
    ]
)
