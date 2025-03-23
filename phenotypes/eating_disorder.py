
from phenotype import PhenoType, ScoreBasedQueryStrategy

class EatingDisorder(PhenoType): #Not to be confused with Generalized Anxiety Disorder

    def __init__(self):


        super().__init__(
            name="EatingDisorder",
            icd9_codes=  ["3071", "3075", "30750", "30751", "30752", "30753", "30754", "30755", "30759"]  ,
            icd10_codes=[
    "F500",  # Anorexia nervosa
    "F501",  # Atypical anorexia nervosa
    "F502",  # Bulimia nervosa
    "F503",  # Atypical bulimia nervosa
    "F508",  # Other eating disorders
    "F509"   # Eating disorder, unspecified
],
            sr_codes=["1470"],
            ever_diag_codes=["13","12","16"]
        )


