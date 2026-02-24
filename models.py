from dataclasses import dataclass

@dataclass
class EvaluationRow:
    id: str
    title: str
    researchers: str
    sectors: str
    fieldwork: str
    location: str
    sample: str
    timeline_start: str
    timeline_end: str
    target_group: str
    outcome_of_interest: str
    intervention_type: str
    research_papers: str
    partners: str
    abstract: str
    policy_issue: str
    context_of_eval: str
    details_inter: str
    results_lessons: str
    citations: str