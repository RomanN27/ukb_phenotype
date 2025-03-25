from typing import List, Tuple

from src.phenotypes import DerivedPhenotype, ScoredBasedDerivedPhenoType, ICD10DerivedPhenoType, ICD9DerivedPhenoType, SelfReportDerivedPhenoType,EverDiagnosedDerivedPhenoType
from src.phenotypes.assigned_phenotype_fields import PhenotypeNames
from src.phenotypes.derived_phenotype import AnyDerivedPhenotype
from src.query_strategy import ScoreBasedQueryStrategy
from src.utils import pcol
from pyspark.sql import DataFrame, Column

from pyspark.sql.functions import col, when
from functools import reduce

from dataclasses import dataclass, field


@dataclass
class ProbableDepression(ScoredBasedDerivedPhenoType):
    name = PhenotypeNames.PROBABLE_DEPRESSION
    score_levels = [1, 2, 3]
    severity_names = ["Single Episode Probable Depression",
                      "Moderate Recurrent Probable Depression",
                      "Severe Recurrent Probable Depression"]
    phenotype_source_fields = [4598, 4609, 4620, 2090, 2100, 4631, 5375, 5386]
    n_instances = 4
    def make_score_column(self)->Column:
        pcol = self.pcol
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



        seen_gp_but_not_psychiatrist = (seen_a_gp & (~ seen_a_psychiatrist))

        being_depressed_long = down_for_whole_week & at_least_two_weeks & (n_depressed_episodes >= 2)
        being_uninterested_long = ever_anhedonic_for_a_week & (n_uninterested_episodes >= 2) & (weeks_uninterested > 2)

        being_down_long = being_depressed_long | being_uninterested_long


        score_1_bool = first_criteria | second_criteria
        score_2_bool = being_down_long & seen_gp_but_not_psychiatrist
        score_3_bool = being_down_long & seen_a_psychiatrist

        return  when(score_1_bool, 1).when(score_2_bool, 2).when(score_3_bool, 3).otherwise(0)

phq9_depression = ScoredBasedDerivedPhenoType(name=PhenotypeNames.PHQ9_DEPRESSION,
                                              phenotype_source_fields=[20514, 20510, 20517, 20519, 20511, 20507, 20508, 20518, 20513],

                                              score_levels=[5, 10, 15, 20],
                                              severity_names=["No", "Low", "Mild", "Moderate", "Severe"]
                                    )
@dataclass
class LifeTimeDepression(DerivedPhenotype):
    name:str = PhenotypeNames.LIFETIME_DEPRESSION
    phenotype_source_fields:  list = field(default_factory=lambda: [20446, 20441, 20436, 20439, 20440, 20449, 20536, 20532, 20435, 20450, 20437])
    def query_boolean_column(self, df: DataFrame) -> Tuple[DataFrame, Column]:
        pcol = self.pcol
        prolonged_sadness = pcol(20446) == 1
        prolonged_loss_interest = pcol(20441) == 1
        fraction_day_affected = pcol(20436).isin(3, 4)
        frequency_depressed_days = pcol(20439).isin(2, 3)
        impairment = pcol(20440).isin(2, 3)
        total_symptoms = (
                prolonged_sadness.cast("int") + prolonged_loss_interest.cast("int") +
                (pcol(20449) == 1).cast("int") + pcol(20536).isin(1
                                                                  , 2, 3).cast("int") +
                (pcol(20532) == 1).cast("int") + (pcol(20435) == 1).cast("int") +
                (pcol(20450) == 1).cast("int") + (pcol(20437) == 1).cast("int")
        )
        meets_criteria = (
                                 prolonged_sadness | prolonged_loss_interest) & fraction_day_affected & frequency_depressed_days & impairment & (
                                 total_symptoms >= 5)
        return df, meets_criteria

life_time_depression = LifeTimeDepression()

icd9_depression = ICD9DerivedPhenoType(name=PhenotypeNames.DEPRESSION.icd9(),phenotype_source_codes=["3119", "3004"])
icd10_depression = ICD10DerivedPhenoType(name=PhenotypeNames.DEPRESSION.icd10(),phenotype_source_codes=["F32.0", "F32.1", "F32.2", "F32.3", "F32.8", "F32.9"])
sr_depression = SelfReportDerivedPhenoType(name=PhenotypeNames.DEPRESSION.sr(),phenotype_source_codes=["1278", "1279", "1280", "1281", "1282", "1283", "1284", "1285", "1286"])
ever_diag_depression = EverDiagnosedDerivedPhenoType(name=PhenotypeNames.DEPRESSION.ever_diag(),phenotype_source_codes=[11])


general_depression = AnyDerivedPhenotype(name = PhenotypeNames.DEPRESSION,phenotype_source_fields=[
    PhenotypeNames.LIFETIME_DEPRESSION, PhenotypeNames.PROBABLE_DEPRESSION, PhenotypeNames.PHQ9_DEPRESSION,
    PhenotypeNames.DEPRESSION.icd9(), PhenotypeNames.DEPRESSION.icd10(), PhenotypeNames.DEPRESSION.sr(),
    PhenotypeNames.DEPRESSION.ever_diag()])