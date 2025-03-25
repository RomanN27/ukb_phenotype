from enum import StrEnum, auto

class PhenotypeNames(StrEnum):
    PROBABLE_DEPRESSION = auto()
    PHQ9_DEPRESSION = auto()
    LIFETIME_DEPRESSION = auto()
    GENERAL_DEPRESSION = auto()
    DEPRESSION = auto()

    def icd9(self):
        return "icd9_" + self.name.lower()

    def icd10(self):
        return "icd10_" + self.name.lower()

    def sr(self):
        return "sr_" + self.name.lower()

    def ever_diag(self):
        return "ever_diag_" + self.name.lower()





