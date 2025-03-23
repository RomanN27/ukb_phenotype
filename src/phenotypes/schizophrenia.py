from src.phenotypes import PhenoType
from src.query_strategy import ScoreBasedQueryStrategy

schizophrenia = PhenoType(name="Schizophrenia",
                         icd9_codes=["2950", "2951", "2952", "2953", "295F4", "2955", "2956", "2957", "2958", "2959"],
                         icd10_codes=[
                             # Schizophrenia
                             "F20.0", "F20.1", "F20.2", "F20.3", "F20.4", "F20.5", "F20.6", "F20.8", "F20.9",

                             # Schizotypal Disorder
                             "F21",

                             # Persistent delusional disorder
                             "F22.0", "F22.8", "F22.9",

                             # Acute and transient psychotic disorders
                             "F23.0", "F23.1", "F23.2", "F23.3", "F23.8", "F23.9",

                             # F24 (Unspecified, included as is)
                             "F24",

                             # Schizoaffective Disorders
                             "F25.0", "F25.1", "F25.2", "F25.8", "F25.9",

                             # Other non-organic psychotic disorders
                             "F28", "F29"
                         ], sr_codes=["1289"], ever_diag_codes=["2"])


