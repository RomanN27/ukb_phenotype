from pyspark.sql import DataFrame
# Initialize Spark session
from src.phenotypes import PhenoType
from src.utils import pcol
from pyspark.sql.functions import *
def self_harm_query(df: DataFrame) -> DataFrame:
    
    # Define conditions for self-harm and suicide risk
    self_harm_1 = (pcol(20480) == 1) | (pcol(20479) == 1) | (size(array_intersect(pcol(20554), array(*[lit(n) for n in [1,3,4,5,6]])))>0)

    suicide_attempt = (pcol(20483) == 1)

    # Create new columns indicating self-harm and suicide attempt cases
    df = df.withColumn("self_harm_case", self_harm_1)
    df = df.withColumn("suicide_attempt_case", suicide_attempt)
    return df


self_harm = PhenoType(name="SelfHarm",
                         icd9_codes=["E950", "E951", "E952", "E953", "E954", "E955", "E956", "E957", "E958", "E959"],
                         icd10_codes=[
                             # Intentional self-harm
                             "X60", "X61", "X62", "X63", "X64", "X65", "X66", "X67", "X68", "X69",
                             "X70", "X71", "X72", "X73", "X74", "X75", "X76", "X77", "X78", "X79",
                             "X80", "X81", "X82", "X83", "X84",

                             # Personal history of self-harm
                             "Z915",

                             # Y1-Y3 range (Assumed to include Y1, Y2, Y3)
                             "Y1", "Y2", "Y3"
                         ], sr_codes=["1290"], associated_field_numbers=[20480,20483, 20479, 20554],
                      query=self_harm_query)






