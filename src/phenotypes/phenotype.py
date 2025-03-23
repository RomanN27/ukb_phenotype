

from typing import Optional, Callable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


from dataclasses import dataclass, field
from typing import List
from functools import reduce


@dataclass
class PhenoType:
    name: str
    icd9_codes: List[str] = field(default_factory=list)
    icd10_codes: List[str] = field(default_factory=list)
    sr_codes: List[str] = field(default_factory=list)
    ever_diag_codes: List[str] = field(default_factory=list)
    associated_field_numbers: List[int] = field(default_factory=list)
    query: Optional[Callable[[DataFrame], DataFrame]] = None

    @staticmethod
    def merge_phenotypes(name:str, *phenotypes:"PhenoType")->"PhenoType":
        icd9_codes= sum([p.icd9_codes for p in phenotypes],[])
        icd10_codes= sum([p.icd10_codes for p in phenotypes],[])
        sr_codes= sum([p.sr_codes for p in phenotypes],[])
        ever_diag_codes= sum([p.ever_diag_codes for p in phenotypes],[])
        associated_field_numbers= sum([p.associated_field_numbers for p in phenotypes],[])

        def query(df:DataFrame)->DataFrame:
            print(associated_field_numbers)
            df =  reduce(lambda df, p: p.query(df) if p.query else df, phenotypes, df)
            phenotype_names = [p.name for p in phenotypes if p.name in df.columns]

            if name in df.columns:
                phenotype_names.append(name)
                df = df.withColumn("Diagnosed" + name, col(name))

            any_phenotype = reduce(lambda x, y: x | y, [col(pname) for pname in phenotype_names])
            df = df.withColumn(name, any_phenotype)
            return df

        return PhenoType(name, icd9_codes, icd10_codes, sr_codes, ever_diag_codes, associated_field_numbers, query)

