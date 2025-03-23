
from phenotype import PhenoType, ScoreBasedQueryStrategy

class OCD(PhenoType): #Not to be confused with Generalized Anxiety Disorder

    def __init__(self):

        super().__init__(
            name="OCD",
            icd9_codes= ["3003"],
            icd10_codes=["F42.0", "F42.1", "F42.2", "F42.8", "F42.9"],
            sr_codes=["1615"],
            ever_diag_codes=["7"]
        )


