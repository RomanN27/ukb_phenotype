import pyspark
from src.phenotypes import (self_harm, eating_disorder, bipolar, general_depression,
                 anxiety, alcohol_abuse, substance_abuse_non_alcoholic, schizophrenia, ptsd, ocd)

from src.query_strategy import PhenotypeQueryManager

def main():
    sc = pyspark.SparkContext()
    spark = pyspark.sql.SparkSession(sc)

    query_manager = PhenotypeQueryManager(spark)
    #make sure that depression is processed before bipolar
    df  =query_manager.query_all(self_harm, eating_disorder, general_depression,bipolar,anxiety,
                                 alcohol_abuse, substance_abuse_non_alcoholic, schizophrenia, ptsd, ocd)


    df = df.toPandas()
    df.to_csv("data.csv")


if __name__ == "__main__":
    main()
