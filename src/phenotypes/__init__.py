from src.phenotypes.derived_phenotype import DerivedPhenotype, ScoredBasedDerivedPhenoType, ICD9DerivedPhenoType, ICD10DerivedPhenoType, SelfReportDerivedPhenoType,EverDiagnosedDerivedPhenoType
from src.phenotypes.eating_disorder import eating_disorder
from src.phenotypes.schizophrenia import schizophrenia
from src.phenotypes.ocd import ocd
from src.phenotypes.anxiety import anxiety

from src.phenotypes.depression import general_depression, depression_name
#make sure to import bipoler after depression since its definition needs depression_name
from src.phenotypes.bipolar import bipolar
from src.phenotypes.ptsd import ptsd
from src.phenotypes.substance_abuse import alcohol_abuse, substance_abuse_non_alcoholic
from src.phenotypes.self_harm import self_harm