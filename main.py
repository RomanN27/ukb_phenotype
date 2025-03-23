from inspect import isclass
from phenotypes import (SelfHarm,EatingDisorder,GeneralBipolar,GeneralDepression,
                        GeneralAnxiety,AlocoholAbuse,SubstanceAbuseNonAlcoholic,Schizophrenia,PTSD,OCD,mother_df)
def main():
    phenotypes = [SelfHarm, EatingDisorder, GeneralBipolar, GeneralDepression,
                  GeneralAnxiety, AlocoholAbuse, SubstanceAbuseNonAlcoholic, Schizophrenia, PTSD, OCD]

    for phenotype in phenotypes:
        if isclass(phenotype):
            phenotype = phenotype()
        phenotype.add_to_mother_df(mother_df)

    df = mother_df.toPandas()
    df.to_csv("data.csv")


if __name__ == "__main__":
    main()
