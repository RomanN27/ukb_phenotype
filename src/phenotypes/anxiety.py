
from src.phenotypes import PhenoType
from src.query_strategy import ScoreBasedQueryStrategy
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


gad7_anxiety_name = "GAD7_Anxiety"

GAD_7_fields = [20506, 20509, 20520, 20515, 20516, 20505, 20512]

def gad7_query(df: DataFrame) -> DataFrame:

    GAD_7_score_names = ["Low", "Mild", "Moderate", "Severe"]
    gad7_score_query = ScoreBasedQueryStrategy(
        field_numbers=GAD_7_fields,
        score_column="GAD7_score",
        risk_column="GAD7_risk",
        score_levels=[5, 10, 15],
        score_level_names=GAD_7_score_names
    )
    df = gad7_score_query(df)

    df = df.withColumn(gad7_anxiety_name, col("GAD7_score") >= 10)
    return df

gad7_anxiety = PhenoType(name=gad7_anxiety_name,
                         associated_field_numbers=GAD_7_fields,
                         query=gad7_query)


diagnosed_anxiety = PhenoType(name="Diagnosed_Anxiety", icd9_codes=["3000", "3001", "3002", "3009", "300"],
                         icd10_codes=["F064", "F93.0", "F931", "F932", "F40", "F400", "F401", "F402", "F408", "F409",
                                      "F41", "F410", "F411",
                                      "F412", "F413", "F418", "F419"], sr_codes=["1287", "1614"],
                         ever_diag_codes=["1", "6", "17"])


anxiety  = PhenoType.merge_phenotypes("Anxiety",gad7_anxiety,diagnosed_anxiety)