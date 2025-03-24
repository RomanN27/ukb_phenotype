from pyspark.sql import DataFrame
from src.phenotypes import DerivedPhenotype
from src.utils import pcol

def ptsd_query(df: DataFrame) -> DataFrame:
    # Calculate PCL-6 score (adjusting values by subtracting 1 for 0-4 scale)
    pcl6_score = (
            (pcol(20497) - 1) +
            (pcol(20498) - 1) +
            (pcol(20495) - 1) +
            (pcol(20496) - 1) +
            (pcol(20494) - 1) +
            (pcol(20508) - 1)
    )

    # Define positive screen for PTSD
    df = df.withColumn("pcl6_score", pcl6_score)
    df = df.withColumn("pcl6_positive_screen", pcl6_score >= 14)
    return df


ptsd = DerivedPhenotype(name="PTSD", icd10_codes=["F431"], sr_codes=["1469"],
                        associated_field_numbers=[20497, 20498, 20495, 20496, 20494, 20508], query=ptsd_query)

