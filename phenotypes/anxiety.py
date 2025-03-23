
from phenotype import PhenoType, ScoreBasedQueryStrategy

class GeneralAnxiety(PhenoType): #Not to be confused with Generalized Anxiety Disorder

    def __init__(self):
        GAD_7_fields = [20506, 20509, 20520, 20515, 20516, 20505, 20512]
        GAD_7_score_names = ["Low", "Mild", "Moderate", "Severe"]
        query_strategy = ScoreBasedQueryStrategy(
            field_numbers=GAD_7_fields,
            score_column="GAD7_score",
            risk_column="GAD7_risk",
            score_levels=[5, 10, 15],
            score_level_names=GAD_7_score_names
        )

        super().__init__(
            name="Anxiety",
            icd9_codes= ["3000", "3001", "3002", "3009", "300"],
            icd10_codes=["F064", "F93.0", "F931", "F932", "F40", "F400", "F401", "F402", "F408", "F409", "F41", "F410", "F411",
            "F412", "F413", "F418", "F419"],
            sr_codes=["1287", "1614"],
            ever_diag_codes=["1", "6", "17"],
            associated_field_numbers=GAD_7_fields,
            query_strategy = query_strategy
        )


