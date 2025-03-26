from typing import Tuple

from pyspark.sql import DataFrame, Column

from src.phenotypes import DerivedPhenotype, VerbalInterviewDerivedPhenoType
from src.phenotypes.derived_phenotype import AnyDerivedPhenotype
from src.phenotypes.phenotype_names import PhenotypeName


def insomnia_query(phenotype:DerivedPhenotype,df: DataFrame) -> Tuple[DataFrame, Column]:
    insomnia = phenotype.pcol(1200) >=2
    return df, insomnia


vi_insomnia = VerbalInterviewDerivedPhenoType(
    name=PhenotypeName.INSOMNIA.vi(),
    phenotype_source_codes=["1616"]
)

touch_screen_insomnia = DerivedPhenotype(name = PhenotypeName.INSOMNIA.touch_screen(),phenotype_source_field_numbers=[1200], query_callable=insomnia_query)

insomnia = AnyDerivedPhenotype(name=PhenotypeName.INSOMNIA, derived_phenotype_sources=[vi_insomnia, touch_screen_insomnia])


