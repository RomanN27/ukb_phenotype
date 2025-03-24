from abc import abstractmethod
from typing import Optional, Callable, Tuple, override
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col, when
from typing import ClassVar

from dataclasses import dataclass, field
from typing import List
from functools import reduce, partial
from src.utils import intersect_arrays, contains_any
from src.query_strategy import ICD_9, ICD_10, SR_20002
from pyspark.sql.functions import col

@dataclass
class DerivedPhenotype:
    name: str
    assigned_field_number: int #for child phenotypes
    phenotype_source_fields: List[int] = field(default_factory=list)
    n_instances: Optional[int] = None


    def pcol(self,field_number: int, instance_number: Optional[int] = None, array_number: Optional[int] = None) -> Column:
        return col(self.p(field_number, instance_number, array_number))

    @staticmethod
    def p(field_number: int, instance_number: Optional[int] = None, array_number: Optional[int] = None) -> str:
        field_name = f"p{field_number}"
        if instance_number is not None:
            field_name += f"_i{instance_number}"
            if array_number is not None:
                field_name += f"_a{array_number}"

        return field_name

    @abstractmethod
    def query_boolean_column(self, df :DataFrame) -> Tuple[DataFrame, Column]:
        raise NotImplementedError()

    def query(self,df :DataFrame)->DataFrame:
        if self.n_instances is not None:
            #  I assume that each source field has the same number of instances
            old_p = self.p
            old_name = self.name
            for i in range(self.n_instances):
                self.name = f"{old_name}_{i}"
                self.p = partial(old_p, instance_number=i)
                df, boolean_column = self.query_boolean_column(df)
                df = df.withColumn(self.name, boolean_column)

            self.p = old_p
            self.name = old_name

        df, boolean_column = self.query_boolean_column(df)
        df = df.withColumn(self.name, boolean_column)
        return df

@dataclass
class CodeDerivedPhenotype(DerivedPhenotype):
    source_field_number: ClassVar[int]
    source_field_name: ClassVar[str]
    phenotype_source_fields: List[int] = field(default_factory=list, init=False)
    phenotype_source_codes: List[str|int] = field(default_factory=list, init=False)

    def __post_init__(self):
        self.name = self.name if self.source_field_name not in self.name.lower() else self.name

    def query_boolean_column(self, df :DataFrame) -> Tuple[DataFrame, Column]:
        bool_col = contains_any(self.pcol(self.source_field_number), self.phenotype_source_codes)
        return df, bool_col



@dataclass
class ICD9DerivedPhenoType(CodeDerivedPhenotype):
    phenotype_source_codes: List[int] = field(default_factory=list, init=False)
    source_field_number:ClassVar[int] = 41271
    source_field_name: ClassVar[str] = "icd9"
    n_instances:  Optional[int]  = field(default=None, init=False)


@dataclass
class ICD10DerivedPhenoType(CodeDerivedPhenotype):
    phenotype_source_codes: List[int] = field(default_factory=list, init=False)
    source_field_number:ClassVar[int] = 41270
    source_field_name: ClassVar[str] = "icd10"
    n_instances: Optional[int] = field(default=None, init=False)

@dataclass
class SelfReportDerivedPhenoType(CodeDerivedPhenotype):
    phenotype_source_codes: List[int] = field(default_factory=list, init=False)
    source_field_number:ClassVar[int] = 20002
    source_field_name: ClassVar[str] = "sr"
    n_instances: Optional[int] = field(default=None, init=False)

@dataclass
class EverDiagnosedDerivedPhenoType(CodeDerivedPhenotype):
    phenotype_source_codes: List[str] = field(default_factory=list, init=False)
    source_field_number:ClassVar[int] = 20544
    source_field_name: ClassVar[str] = "ever_diag"
    n_instances: int = field(default=4, init=False)

@dataclass
class ScoredBasedDerivedPhenoType(DerivedPhenotype):
    score_levels: List[int] = field(default_factory=list)
    severity_names: Optional[List[str]] = None

    def preprocess_score_columns(self, df: DataFrame) -> DataFrame:
        return df

    @property
    def score_name(self) -> str:
        return self.name + "_score"

    def score_to_boolean(self, score: Column) -> Column:
        return score >= self.score_levels[-1]

    def query_boolean_column(self, df :DataFrame) -> Tuple[DataFrame, Column]:
        df = self.preprocess_score_columns(df)
        df = df.withColumn(self.score_name, sum([self.pcol(x) for x in self.phenotype_source_fields]))

        boundaries = [(float("-inf"), self.score_levels[0])] + \
                     [(self.score_levels[i], self.score_levels[i + 1]) for i in range(len(self.score_levels) - 1)] + \
                     [(self.score_levels[-1], float("inf"))]

        if self.severity_names is not None:
            risk_expr = when(col(self.score_name).isNotNull(), None)  # Default case
            for (min_val, max_val), label in zip(boundaries, self.severity_names):
                risk_expr = risk_expr.when((col(self.score_name) >= min_val) & (col(self.score_name) < max_val),
                                           label)

        return df, self.score_to_boolean(col(self.score_name))

