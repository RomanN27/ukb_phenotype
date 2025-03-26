from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col, when, size

from src.phenotypes import ScoredBasedDerivedPhenoType, general_depression, get_min_score_to_boolean
from src.phenotypes.phenotype_names import PhenotypeName
from src.phenotypes.derived_phenotype import AnyDerivedPhenotype

def probable_bipolar_scorer(phenotype:ScoredBasedDerivedPhenoType) -> Column:
    probable_bipolar_ii=when((
            (phenotype.pcol(4642) == 1) | (phenotype.pcol(4653) == 1)  # Condition 1: Manic/hypomanic symptoms
    ) &
    (
            size(phenotype.pcol(6156)) >= 3  # Condition 2: At least three symptoms
    ) &
    (
        (phenotype.pcol(5663) == 13)  # Condition 3: Specific symptom criteria
    ), True).otherwise(False)


    score = probable_bipolar_ii.cast("int")
    score += (phenotype.pcol(5674) == 12).cast("int")
    return score


probable_bipolar = ScoredBasedDerivedPhenoType(PhenotypeName.PROBABLE_BIPOLAR,
                                   score_levels=[1, 2],
                                   severity_names=["No Probable Bipolar","Probable Bipolar II", "Probable Bipolar I"],
                                   phenotype_source_field_numbers=[4642, 4653, 6156, 5663, 5674],
                                   n_instances=4,
                                               make_score_column=probable_bipolar_scorer)

def life_time_bipolar_scorer(phenotype:ScoredBasedDerivedPhenoType):


    bipolar_affective_disorder_ii = when(
        (col(general_depression.name) == 1) &  # Prior depression phenotype
        (
                (phenotype.pcol(20501) == 1) | (phenotype.pcol(20502) == 1)  # Mania or extreme irritability
        ) &
        (
            (size(phenotype.pcol(20548)) >= 4)  # At least four symptoms
        ) &
        (
                phenotype.pcol(20492) == 3  # Period of mania or irritability
        ), True).otherwise(False)


    score = bipolar_affective_disorder_ii.cast("int")
    score += (phenotype.pcol(20493) == 12).cast("int")
    return score


life_time_bipolar =ScoredBasedDerivedPhenoType(PhenotypeName.LIFE_TIME_BIPOLAR,
                                    score_levels=[1, 2],
                                    severity_names=["No Life Time Bipolar","Life Time Bipolar II", "Life Time Bipolar I"],
                                    phenotype_source_field_numbers=[4642, 4653, 6156, 5663, 5674],
                                    derived_phenotype_sources=[general_depression],
                                               make_score_column=life_time_bipolar_scorer,
                                               score_to_boolean=get_min_score_to_boolean(0))



bipolar  = AnyDerivedPhenotype(name=PhenotypeName.BIPOLAR, derived_phenotype_sources=[life_time_bipolar, probable_bipolar])