from src.phenotypes import ScoredBasedDerivedPhenoType
from src.phenotypes.phenotype_names import PhenotypeName

neuroticism  = ScoredBasedDerivedPhenoType(
    name = PhenotypeName.NEUROTICISM,
    phenotype_source_field_numbers=[20127],
    score_levels=[3,7,11],
    severity_names=["No", "Low", "Moderate", "High"]
)