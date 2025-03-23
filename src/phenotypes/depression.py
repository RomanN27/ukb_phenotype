from phenotype import PhenoType
from src import ScoreBasedQueryStrategy
from src import pcol
from pyspark.sql import DataFrame


def probable_depression_query(df: DataFrame) -> DataFrame:
    down_for_whole_week = pcol(4598) == 1
    at_least_two_weeks = pcol(4609) == 1
    n_depressed_episodes = pcol(4620)
    ever_anhedonic_for_a_week = pcol(4631) == 1
    seen_a_gp = pcol(2090) == 1
    seen_a_psychiatrist = pcol(2100) == 1
    weeks_uninterested = pcol(5375)
    n_uninterested_episodes = pcol(5386)

    first_criteria = down_for_whole_week & at_least_two_weeks & (n_depressed_episodes == 1) & (
            seen_a_gp | seen_a_psychiatrist)

    second_criteria = ever_anhedonic_for_a_week & (n_uninterested_episodes == 1) & (weeks_uninterested >= 2)

    df = df.withColumn(
        "single_probable_episode_of_major_depression",
        (
                first_criteria | second_criteria
        )
    )

    seen_gp_but_not_psychiatrist = (seen_a_gp & (not seen_a_psychiatrist))

    being_depressed_long = down_for_whole_week & at_least_two_weeks & (n_depressed_episodes >= 2)
    being_uninterested_long = ever_anhedonic_for_a_week & (n_uninterested_episodes >= 2) & (weeks_uninterested > 2)

    being_down_long = being_depressed_long | being_uninterested_long

    df = df.withColumn(
        "probable_recurrent_major_depression_moderate",
        (
                being_down_long & seen_gp_but_not_psychiatrist
        )
    )

    df = df.withColumn(
        "probable_recurrent_major_depression_severe",
        (
                being_down_long & seen_a_psychiatrist
        )
    )

    return df

probable_depression = PhenoType(
    name="ProbableDepression",
    associated_field_numbers=[4598, 4609, 4620, 2090, 2100, 4631, 5375, 5386],
    query=probable_depression_query
)


phq_9_field_numbers = [20514, 20510, 20517, 20519, 20511, 20507, 20508, 20518, 20513]
phq_9_depression_query = ScoreBasedQueryStrategy(
            field_numbers=phq_9_field_numbers,
            score_column="PHQ-9-Score",
            risk_column="Recent Diagnosed Depression Severity",
            score_levels=[5, 10, 15, 20],
            score_level_names=["No", "Low", "Mild", "Moderate", "Severe"]
        )

phq_9_depression = PhenoType(name="ProbableDepression", associated_field_numbers=phq_9_field_numbers,query=phq_9_depression_query)


def life_time_depression_query(df: DataFrame) -> DataFrame:
    prolonged_sadness = pcol(20446) == 1
    prolonged_loss_interest = pcol(20441) == 1
    fraction_day_affected = pcol(20436).isin(3, 4)
    frequency_depressed_days = pcol(20439).isin(2, 3)
    impairment = pcol(20440).isin(2, 3)
    total_symptoms = (
            prolonged_sadness.cast("int") + prolonged_loss_interest.cast("int") +
            (pcol(20449) == 1).cast("int") + pcol(20536).isin(1, 2, 3).cast("int") +
            (pcol(20532) == 1).cast("int") + (pcol(20435) == 1).cast("int") +
            (pcol(20450) == 1).cast("int") + (pcol(20437) == 1).cast("int")
    )
    meets_criteria = (
                             prolonged_sadness | prolonged_loss_interest) & fraction_day_affected & frequency_depressed_days & impairment & (
                             total_symptoms >= 5)
    return df.withColumn("LifetimeDepression", meets_criteria)

life_time_depression = PhenoType(name = "LifetimeDepression",
                                 associated_field_numbers=[20446, 20441, 20436, 20439, 20440, 20449, 20536, 20532, 20435, 20450, 20437],
                                 query=life_time_depression_query)


diagnosed_depression = PhenoType(name="DiagnosedDepression", icd9_codes=["3119", "3004"],
                         icd10_codes=[
                             # Depressive Episode
                             "F32.0", "F32.1", "F32.2", "F32.3", "F32.8", "F32.9",

                             # Recurrent Depressive Disorder
                             "F33.0", "F33.4", "F33.8", "F33.9"
                         ], sr_codes=["1278", "1279", "1280", "1281", "1282", "1283", "1284", "1285", "1286"],
                         ever_diag_codes=["11"])



general_depression = PhenoType.merge_phenotypes("GeneralDepression",probable_depression,phq_9_depression,life_time_depression,diagnosed_depression)
