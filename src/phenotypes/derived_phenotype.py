from abc import abstractmethod
from typing import Optional, Callable, Tuple
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col, when
from typing import ClassVar

from dataclasses import dataclass, field, MISSING
from typing import List
from functools import reduce, partial

from src.phenotypes.phenotype_names import PhenotypeName
from src.utils import intersect_arrays, contains_any
from pyspark.sql.functions import col

@dataclass
class DerivedPhenotype:
    name: PhenotypeName
    phenotype_source_field_numbers: List[int] = field(default_factory=list)
    derived_phenotype_sources: List["DerivedPhenotype"] = field(default_factory=list)
    query_callable: Optional[Callable[["DerivedPhenotype",DataFrame], Tuple[DataFrame,Column]]] = None
    n_instances: Optional[int] = None

    @staticmethod
    def pcol(field_number: int|str, instance_number: Optional[int] = None, array_number: Optional[int] = None) -> Column:

        return col(DerivedPhenotype.p(field_number, instance_number, array_number))

    @staticmethod
    def p(field_number: int|str, instance_number: Optional[int] = None, array_number: Optional[int] = None) -> str:

        if isinstance(field_number, str):
            return field_number

        field_name = f"p{field_number}"
        if instance_number is not None:
            field_name += f"_i{instance_number}"
            if array_number is not None:
                field_name += f"_a{array_number}"

        return field_name

    @abstractmethod
    def query_boolean_column(self, df :DataFrame) -> Tuple[DataFrame, Column]:
        if self.query is None:
            raise NotImplementedError()

        return self.query_callable(self,df)

    def query_instances(self,df:DataFrame) -> DataFrame:
        old_p = self.p
        old_p_col = self.pcol
        old_name = self.name
        for i in range(self.n_instances):
            df  = self.query_instance(df, i, old_name, old_p,old_p_col)

        self.p = old_p
        self.p_col = old_p_col
        self.name = old_name
        return df

    def query_instance(self, df, i, old_name, old_p,old_p_col):
        self.name = f"{old_name}_{i}"
        self.p = partial(old_p, instance_number=i)
        self.p_col = partial(old_p_col, instance_number=i)
        df, boolean_column = self.query_boolean_column(df)
        df = df.withColumn(self.name, boolean_column)
        return df

    def query(self,df :DataFrame)->DataFrame:

        for derived_phenotype in self.derived_phenotype_sources:
            if derived_phenotype.name not in df.columns:
                df = derived_phenotype.query(df)

        if self.n_instances is not None:
           df = self.query_instances(df)

        df, boolean_column = self.query_boolean_column(df)
        df = df.withColumn(self.name, boolean_column)
        return df


@dataclass
class OneBooleanFieldPhenotype(DerivedPhenotype):
    source_field_number: int = field(default_factory=int)
    phenotype_source_field_numbers: list[str] = field(default_factory=list, init=False)

    def query_boolean_column(self, df:DataFrame) -> Tuple[DataFrame, Column]:
        return df, self.pcol(self.source_field_number) == 1



@dataclass
class AnyDerivedPhenotype(DerivedPhenotype):
    def query_boolean_column(self, df: DataFrame) -> Tuple[DataFrame, Column]:
        derived_phenotype_names = [x.name for x in self.derived_phenotype_sources]
        return df, reduce(lambda x, y: x | y, [self.pcol(x) for x in self.phenotype_source_field_numbers + derived_phenotype_names])

@dataclass
class CodeDerivedPhenotype(DerivedPhenotype):
    source_field_number: ClassVar[int]
    phenotype_source_field_numbers: List[int] = field(default_factory=list, init=False)
    phenotype_source_codes: List[str|int] = field(default_factory=list)

    def __post_init__(self):
        self.phenotype_source_field_numbers = [self.source_field_number]


    def query_boolean_column(self, df :DataFrame) -> Tuple[DataFrame, Column]:
        bool_col = contains_any(self.pcol(self.source_field_number), self.phenotype_source_codes)
        return df, bool_col



@dataclass
class ICD9DerivedPhenoType(CodeDerivedPhenotype):
    phenotype_source_codes: List[str] = field(default_factory=list)
    source_field_number:ClassVar[int] = 41271
    n_instances:  Optional[int]  = field(default=None, init=False)





@dataclass
class ICD10DerivedPhenoType(CodeDerivedPhenotype):
    phenotype_source_codes: List[str] = field(default_factory=list)
    source_field_number:ClassVar[int] = 41270
    n_instances: Optional[int] = field(default=None, init=False)


@dataclass
class VerbalInterviewDerivedPhenoType(CodeDerivedPhenotype):
    phenotype_source_codes: List[int] = field(default_factory=list)
    source_field_number:ClassVar[int] = 20002
    n_instances: Optional[int] = field(default=None, init=False)



@dataclass
class EverDiagnosedDerivedPhenoType(CodeDerivedPhenotype):
    phenotype_source_codes: List[int] = field(default_factory=list)
    source_field_number:ClassVar[int] = 20544
    n_instances: int = field(default=4, init=False)




def replace_missing_values(phenotype: "ScoredBasedDerivedPhenoType", df: DataFrame) -> DataFrame:
    # Replace missing values (-818) with 0
    for field_number in phenotype.phenotype_source_field_numbers:
        df = df.withColumn(phenotype.p(field_number), when(col(phenotype.p(field_number)) == -818, 0).otherwise(col(phenotype.p(field_number))))
    return df

def get_min_score_to_boolean(min_index:int)->Callable[["ScoredBasedDerivedPhenoType", Column], Column]:
    def min_score_to_boolean(phenotype: "ScoredBasedDerivedPhenoType", score_column: Column) -> Column:
        return score_column > phenotype.score_levels[min_index]
    return min_score_to_boolean


def sum_scorer(phenotype: "ScoredBasedDerivedPhenoType") -> Column:
    return sum([phenotype.pcol(x) for x in phenotype.phenotype_source_field_numbers])


@dataclass
class ScoredBasedDerivedPhenoType(DerivedPhenotype):
    score_levels: List[int] = field(default_factory=list)
    severity_names: Optional[List[str]] = None
    score_name : Optional[str] = None
    severity_name : Optional[str] = None
    preprocess_score_columns: Callable[["ScoredBasedDerivedPhenoType", DataFrame], DataFrame] = replace_missing_values
    make_score_column: Callable[["ScoredBasedDerivedPhenoType "], Column] = sum_scorer
    score_to_boolean: Callable[["ScoredBasedDerivedPhenoType", Column], Column] = get_min_score_to_boolean(-1)
    query_callable: Callable[["ScoredBasedDerivedPhenoType", DataFrame], Tuple[DataFrame,Column]] = field(init=False,default=None)



    def query_instance(self, df: DataFrame, i: int, old_name: str, old_p: str,old_pcol:str) -> DataFrame:
        old_score_name = self.phenotype.score_name
        old_severity_name = self.phenotype.severity_name

        self.phenotype.score_name = f"{old_score_name}_{i}"
        self.phenotype.severity_name = f"{old_severity_name}_{i}"

        df = self.phenotype.query_instance(df, i, old_name, old_p,old_pcol)

        self.phenotype.score_name = old_score_name
        self.phenotype.severity_name = old_severity_name

        return df

    def score_query(self, df: DataFrame) -> Tuple[DataFrame, Column]:
        df = self.preprocess_score_columns(self, df)
        df = df.withColumn(self.score_name, self.make_score_column(self))

        boundaries = [(float("-inf"), self.score_levels[0])] + \
                     [(self.score_levels[i], self.score_levels[i + 1]) for i in range(len(self.score_levels) - 1)] + \
                     [(self.score_levels[-1], float("inf"))]

        if self.severity_names is not None:
            risk_expr = when(col(self.score_name).isNotNull(), None)  # Default case
            for (min_val, max_val), label in zip(boundaries, self.severity_names):
                risk_expr = risk_expr.when((col(self.severity_name) >= min_val) &
                                           (col(self.severity_name) < max_val),
                                           label)

        return df, self.score_to_boolean(self, col(self.score_name))

    def __call__(self, phenotype: DerivedPhenotype, df: DataFrame) -> Tuple[DataFrame, Column]:
        self.phenotype = phenotype
        return self.query_boolean_column(df)

