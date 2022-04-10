import os

from typing import Dict, List

from dagster import (
    AssetGroup,
    asset)
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project
from dagster_gcp.gcs import gcs_pickle_asset_io_manager
from dagster_gcp import gcs_resource

from resources.edfi_api_resource import edfi_api_resource_client


edfi_api_endpoints = [
    {"asset": "base_edfi_assessments", "endpoints": ["/ed-fi/assessments", "/ed-fi/assessments/deletes"]},
    {"asset": "base_edfi_local_education_agencies", "endpoints": ["/ed-fi/localEducationAgencies", "/ed-fi/localEducationAgencies/deletes"]},
    {"asset": "base_edfi_calendars", "endpoints": ["/ed-fi/calendars", "/ed-fi/calendars/deletes"]},
    {"asset": "base_edfi_calendar_dates", "endpoints": ["/ed-fi/calendarDates", "/ed-fi/calendarDates/deletes"]},
    {"asset": "base_edfi_courses", "endpoints": ["/ed-fi/courses", "/ed-fi/courses/deletes"]},
    {"asset": "base_edfi_course_offerings", "endpoints": ["/ed-fi/courseOfferings", "/ed-fi/courseOfferings/deletes"]},
    {"asset": "base_edfi_discipline_actions", "endpoints": ["/ed-fi/disciplineActions", "/ed-fi/disciplineActions/deletes"]},
    {"asset": "base_edfi_discipline_incidents", "endpoints": ["/ed-fi/disciplineIncidents", "/ed-fi/disciplineIncidents/deletes"]},
    {"asset": "base_edfi_grades", "endpoints": ["/ed-fi/grades", "/ed-fi/grades/deletes"]},
    {"asset": "base_edfi_grading_periods", "endpoints": ["/ed-fi/gradingPeriods", "/ed-fi/gradingPeriods/deletes"]},
    {"asset": "base_edfi_grading_period_descriptors", "endpoints": [ "/ed-fi/gradingPeriodDescriptors", "/ed-fi/gradingPeriodDescriptors/deletes"]},
    {"asset": "base_edfi_objective_assessments", "endpoints": ["/ed-fi/objectiveAssessments", "/ed-fi/objectiveAssessments/deletes"]},
    {"asset": "base_edfi_parents", "endpoints": ["/ed-fi/parents", "/ed-fi/parents/deletes"]},
    {"asset": "base_edfi_programs", "endpoints": ["/ed-fi/programs", "/ed-fi/programs/deletes"]},
    {"asset": "base_edfi_schools", "endpoints": ["/ed-fi/schools", "/ed-fi/schools/deletes"]},
    {"asset": "base_edfi_school_year_types", "endpoints": [ "/ed-fi/schoolYearTypes"]},
    {"asset": "base_edfi_sections", "endpoints": ["/ed-fi/sections", "/ed-fi/sections/deletes"]},
    {"asset": "base_edfi_staffs", "endpoints": ["/ed-fi/staffs", "/ed-fi/staffs/deletes"]},
    {"asset": "base_edfi_staff_discipline_incident_associations", "endpoints": ["/ed-fi/staffDisciplineIncidentAssociations", "/ed-fi/staffDisciplineIncidentAssociations/deletes"]},
    {"asset": "base_edfi_staff_education_organization_assignment_associations", "endpoints": ["/ed-fi/staffEducationOrganizationAssignmentAssociations", "/ed-fi/staffEducationOrganizationAssignmentAssociations/deletes"]},
    {"asset": "base_edfi_staff_school_associations", "endpoints": ["/ed-fi/staffSchoolAssociations", "/ed-fi/staffSchoolAssociations/deletes"]},
    {"asset": "base_edfi_staff_section_associations", "endpoints": ["/ed-fi/staffSectionAssociations", "/ed-fi/staffSectionAssociations/deletes"]},
    {"asset": "base_edfi_students", "endpoints": ["/ed-fi/students", "/ed-fi/students/deletes"]},
    {"asset": "base_edfi_student_education_organization_associations", "endpoints": ["/ed-fi/studentEducationOrganizationAssociations", "/ed-fi/studentEducationOrganizationAssociations/deletes"]},
    {"asset": "base_edfi_student_school_associations", "endpoints": ["/ed-fi/studentSchoolAssociations", "/ed-fi/studentSchoolAssociations/deletes"]},
    {"asset": "base_edfi_student_assessments", "endpoints": ["/ed-fi/studentAssessments", "/ed-fi/studentAssessments/deletes", ]},
    {"asset": "base_edfi_student_discipline_incident_associations", "endpoints": ["/ed-fi/studentDisciplineIncidentAssociations", "/ed-fi/studentDisciplineIncidentAssociations/deletes"]},
    {"asset": "base_edfi_student_parent_associations", "endpoints": ["/ed-fi/studentParentAssociations", "/ed-fi/studentParentAssociations/deletes"]},
    {"asset": "base_edfi_student_program_associations", "endpoints": [ "/ed-fi/studentProgramAssociations", "/ed-fi/studentProgramAssociations/deletes"]},
    {"asset": "base_edfi_student_school_attendance_events", "endpoints": ["/ed-fi/studentSchoolAttendanceEvents", "/ed-fi/studentSchoolAttendanceEvents/deletes"]},
    {"asset": "base_edfi_student_section_associations", "endpoints": ["/ed-fi/studentSectionAssociations", "/ed-fi/studentSectionAssociations/deletes"]},
    {"asset": "base_edfi_student_section_attendance_events", "endpoints": [ "/ed-fi/studentSectionAttendanceEvents", "/ed-fi/studentSectionAttendanceEvents/deletes"]},
    {"asset": "base_edfi_student_special_education_program_associations", "endpoints": [ "/ed-fi/studentSpecialEducationProgramAssociations", "/ed-fi/studentSpecialEducationProgramAssociations/deletes"]},
    {"asset": "base_edfi_sessions", "endpoints": [ "/ed-fi/sessions", "/ed-fi/sessions/deletes"]},
    {"asset": "base_edfi_descriptors", "endpoints": ["/ed-fi/cohortTypeDescriptors", "/ed-fi/cohortTypeDescriptors/deletes",
        "/ed-fi/disabilityDescriptors", "/ed-fi/disabilityDescriptors/deletes",
        "/ed-fi/languageDescriptors", "/ed-fi/languageDescriptors/deletes",
        "/ed-fi/languageUseDescriptors", "/ed-fi/languageUseDescriptors/deletes",
        "/ed-fi/raceDescriptors", "/ed-fi/raceDescriptors/deletes"]}
    # {"endpoint": "/ed-fi/studentDisciplineIncidentBehaviorAssociations", "table_name": "base_edfi_student_discipline_incident_behavior_associations" }, # implemented in v5.2
    # {"endpoint": "/ed-fi/studentDisciplineIncidentNonOffenderAssociations", "table_name": "base_edfi_student_discipline_incident_non_offender_associations" }, # implemented in v5.2
    # {"endpoint": "/ed-fi/surveys", "table_name": "base_edfi_surveys" },
    # {"endpoint": "/ed-fi/surveys/deletes", "table_name": "base_edfi_surveys" },
    # {"endpoint": "/ed-fi/surveyQuestions", "table_name": "base_edfi_survey_questions" },
    # {"endpoint": "/ed-fi/surveyQuestions/deletes", "table_name": "base_edfi_survey_questions" },
    # {"endpoint": "/ed-fi/surveyResponses", "table_name": "base_edfi_survey_responses" },
    # {"endpoint": "/ed-fi/surveyResponses/deletes", "table_name": "base_edfi_survey_responses" },
    # {"endpoint": "/ed-fi/surveyQuestionResponses", "table_name": "base_edfi_survey_question_responses" },
    # {"endpoint": "/ed-fi/surveyQuestionResponses/deletes", "table_name": "base_edfi_survey_question_responses" },
]

dbt_assets = load_assets_from_dbt_project(
    project_dir=os.getenv("DBT_PROJECT_DIR"),
    profiles_dir=os.getenv("DBT_PROFILES_DIR"),
    select="tag:edfi"
)

edfi_assets = list()
for edfi_asset in edfi_api_endpoints:

    asset_key = edfi_asset["asset"]

    @asset(
        name=asset_key,
        required_resource_keys={"edfi_api_client"},
        compute_kind="python")
    def extract_and_load_data(context):

        for api_endpoint in edfi_asset["endpoints"]:
            for yielded_response in context.resources.edfi_api_client.get_data(
                api_endpoint, 2022, None, None
            ):

                for record in yielded_response:
                    context.log.debug(record)
    
    edfi_assets.append(extract_and_load_data)


asset_group = AssetGroup(
    edfi_assets + dbt_assets,
    resource_defs={
        "io_manager": gcs_pickle_asset_io_manager.configured(
            {"gcs_bucket": "dagster-test", "gcs_prefix": "asset_folder"}
        ),
        "gcs": gcs_resource,
        "dbt": dbt_cli_resource.configured({
            "project_dir": os.getenv("DBT_PROJECT_DIR"),
            "profiles_dir": os.getenv("DBT_PROFILES_DIR"),
            "target": "dev"
        }),
        "edfi_api_client": edfi_api_resource_client.configured(
            {
                "base_url": os.getenv("EDFI_BASE_URL"),
                "api_key": os.getenv("EDFI_API_KEY"),
                "api_secret": os.getenv("EDFI_API_SECRET"),
                "api_page_limit": 100,
                "api_mode": "SharedInstance",
            }
        ),
    },
)


asset_job = asset_group.build_job(name="edfi_assets")
