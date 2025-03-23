from typing import Optional

from pyspark.sql import Column
from pyspark.sql.functions import  col


def p(field_number:int,instance_number:Optional[int]=None, array_number: Optional[int]=None)->str:
    field_name = f"p{field_number}"
    if instance_number is not None:
        field_name += f"_i{instance_number}"
        if array_number is not None:
            field_name += f"_a{array_number}"

    return field_name


def pcol(field_number:int,instance_number:Optional[int]=None, array_number: Optional[int]=None)->Column:
    return col(p(field_number,instance_number,array_number))
