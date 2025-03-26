import pyspark
from src.phenotypes import (self_harm, eating_disorder, bipolar, general_depression,
                 anxiety, alcohol_abuse, substance_abuse_non_alcoholic, schizophrenia, ptsd, ocd,insomnia,neuroticism)

from src.query_strategy import PhenotypeQueryManager

def main():
    sc = pyspark.SparkContext()
    spark = pyspark.sql.SparkSession(sc)

    query_manager = PhenotypeQueryManager(spark)

    df  =query_manager.query(self_harm, eating_disorder, general_depression,bipolar,anxiety,
                                 alcohol_abuse, substance_abuse_non_alcoholic, schizophrenia, ptsd, ocd,insomnia,neuroticism)


    df = df.toPandas()
    df.to_csv("data.csv")


if __name__ == "__main__":
    main()
