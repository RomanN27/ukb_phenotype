from typing import Optional

from pyspark.sql import Column
from pyspark.sql.functions import col, array, lit, arrays_overlap


def p(field_number:int,instance_number:Optional[int]=None, array_number: Optional[int]=None)->str:
    field_name = f"p{field_number}"
    if instance_number is not None:
        field_name += f"_i{instance_number}"
        if array_number is not None:
            field_name += f"_a{array_number}"

    return field_name


def pcol(field_number:int,instance_number:Optional[int]=None, array_number: Optional[int]=None)->Column:
    return col(p(field_number,instance_number,array_number))

def contains_any(array_column: Column,values:list)->Column:

    return arrays_overlap(array_column,  array(*[lit(n) for n in values]))
