from src.phenotypes import DerivedPhenotype, ICD9DerivedPhenoType, ICD10DerivedPhenoType, VerbalInterviewDerivedPhenoType, EverDiagnosedDerivedPhenoType, AnyDerivedPhenotype
from src.phenotypes.phenotype_names import PhenotypeName

icd9_eating_disorder = ICD9DerivedPhenoType(name=PhenotypeName.EATING_DISORDER.icd9(), phenotype_source_codes=["3071", "3075", "30750", "30751", "30752", "30753", "30754", "30755", "30759"])
icd10_eating_disorder = ICD10DerivedPhenoType(name=PhenotypeName.EATING_DISORDER.icd10(), phenotype_source_codes=[
                             "F500",  # Anorexia nervosa
                             "F501",  # Atypical anorexia nervosa
                             "F502",  # Bulimia nervosa
                             "F503",  # Atypical bulimia nervosa
                             "F508",  # Other eating disorders
                             "F509"  # Eating disorder, unspecified
                         ])

sr_eating_disorder = VerbalInterviewDerivedPhenoType(name=PhenotypeName.EATING_DISORDER.vi(), phenotype_source_codes=["1470"])
ever_diag_eating_disorder = EverDiagnosedDerivedPhenoType(name=PhenotypeName.EATING_DISORDER.ever_diag(), phenotype_source_codes=[13, 12, 16])

eating_disorder = AnyDerivedPhenotype(name=PhenotypeName.EATING_DISORDER, derived_phenotype_sources=[icd9_eating_disorder, icd10_eating_disorder, sr_eating_disorder, ever_diag_eating_disorder])