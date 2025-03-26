from enum import StrEnum, auto

class PhenotypeName(StrEnum):
    PROBABLE_DEPRESSION = auto()
    PHQ9_DEPRESSION = auto()
    LIFETIME_DEPRESSION = auto()
    GENERAL_DEPRESSION = auto()
    DEPRESSION = auto()

    EATING_DISORDER = auto()

    PTSD = auto()
    PTSD_PCL6 = auto()

    SCHIZOPHRENIA = auto()

    ANXIETY = auto()
    ANXIETY_GAD7 = auto()

    SELF_HARM = auto()
    QUESTIONNAIRE_SELF_HARM = auto()

    OCD = auto()

    SUBSTANCE_ABUSE_NON_ALCOHOLIC = auto()
    QUESTIONNAIRE_SUBSTANCE_ABUSE_NON_ALCOHOLIC = auto()

    AUDIT_SCORE = auto()
    ALCOHOL_ADDICTION = auto()
    ALCOHOL_ABUSE = auto()

    PROBABLE_BIPOLAR = auto()
    LIFE_TIME_BIPOLAR = auto()
    BIPOLAR = auto()

    INSOMNIA = auto()

    NEUROTICISM = auto()

    def icd9(self):
        return "icd9_" + self.name.lower()

    def icd10(self):
        return "icd10_" + self.name.lower()

    def vi(self):
        return "verbal_interview_" + self.name.lower()

    def ever_diag(self):
        return "ever_diag_" + self.name.lower()

    def touch_screen(self):
        return "touch_screen_" + self.name.lower()





