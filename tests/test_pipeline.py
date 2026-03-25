# ============================================
# PROJECT 101 - PYTEST TESTS
# Purpose: Validate pipeline functions work
#          correctly before running on real data
# ============================================

import pytest
import pandas as pd
import numpy as np
import os
import sys

# Add pipeline folder to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'pipeline'))


# ============================================
# TEST DATA (Fake data that mimics real CSV)
# ============================================

@pytest.fixture
def sample_raw_df():
    """
    Create a small fake DataFrame that mimics
    the Stack Overflow 2020 CSV structure.
    Used by multiple tests below.
    """
    return pd.DataFrame({
        'Respondent'              : ['1', '2', '3', '4', '5'],
        'MainBranch'              : [
            'I am a developer by profession',
            'I am a student',
            'I am a developer by profession',
            None,
            'I code primarily as a hobby'
        ],
        'Hobbyist'                : ['Yes', 'No', 'Yes', 'Yes', None],
        'Age'                     : ['25-34 years old', '18-24 years old',
                                     '35-44 years old', None, '25-34 years old'],
        'Gender'                  : ['Man', 'Woman', 'Man;Non-binary',
                                     'Man', None],
        'Country'                 : ['United States', 'Nigeria', 'Germany',
                                     'Cameroon', 'India'],
        'Ethnicity'               : ['White', 'Black', 'White', None, 'Asian'],
        'EdLevel'                 : [
            "Bachelor's degree",
            "Master's degree",
            "Bachelor's degree",
            None,
            "Some college"
        ],
        'UndergradMajor'          : ['Computer Science', 'Information Systems',
                                     'Computer Science', None, 'Engineering'],
        'Employment'              : ['Employed full-time', 'Student',
                                     'Employed full-time', 'Employed part-time',
                                     'Employed full-time'],
        'OrgSize'                 : ['100 to 499 employees', '20 to 99 employees',
                                     'Just me', None, '1,000 to 4,999 employees'],
        'DevType'                 : ['Back-end developer;Full-stack developer',
                                     'Student', 'Front-end developer',
                                     'DBA/sysadmin', 'Data scientist'],
        'YearsCode'               : ['5', '2', 'Less than 1 year',
                                     'More than 50 years', '10'],
        'YearsCodePro'            : ['3', None, '0', '30', '7'],
        'Currency'                : ['USD', 'NGN', 'EUR', 'XAF', 'INR'],
        'CompTotal'               : ['95000', '500000', '75000', None, '1200000'],
        'CompFreq'                : ['Yearly', 'Monthly', 'Yearly',
                                     None, 'Yearly'],
        'LanguageWorkedWith'      : ['Python;SQL;JavaScript',
                                     'Python;Java',
                                     'JavaScript;TypeScript',
                                     'SQL;T-SQL',
                                     'Python;R;SQL'],
        'LanguageDesireNextYear'  : ['Python;Rust', 'Python;Go',
                                     'Python', 'Python;SQL', 'Julia;Python'],
        'DatabaseWorkedWith'      : ['MySQL;PostgreSQL', 'MySQL',
                                     'MongoDB', 'SQL Server', 'MySQL;Redis'],
        'DatabaseDesireNextYear'  : ['PostgreSQL', 'PostgreSQL',
                                     'MongoDB', 'MySQL', 'PostgreSQL'],
        'PlatformWorkedWith'      : ['AWS', 'Azure', 'AWS;GCP',
                                     'Azure', 'AWS'],
        'PlatformDesireNextYear'  : ['AWS', 'AWS', 'AWS', 'Azure', 'GCP'],
        'WebFrameWorkedWith'      : ['React;Django', 'Spring', 'React;Vue.js',
                                     None, 'Django;Flask'],
        'WebFrameDesireNextYear'  : ['FastAPI', 'React', 'Vue.js',
                                     None, 'FastAPI'],
        'JobSat'                  : ['Very satisfied', 'Slightly satisfied',
                                     'Neither satisfied nor dissatisfied',
                                     'Slightly dissatisfied', 'Very satisfied'],
        'CareerSat'               : ['Very satisfied', 'Slightly satisfied',
                                     'Very satisfied', None, 'Very satisfied'],
        'WorkWeekHrs'             : ['40', '20', '45', '50', '38'],
        'OpSys'                   : ['Windows', 'Linux-based',
                                     'MacOS', 'Windows', 'Linux-based'],
    })


@pytest.fixture
def sample_raw_db_df():
    """
    Fake DataFrame mimicking what comes
    OUT of SQL Server (column names already mapped).
    """
    return pd.DataFrame({
        'respondent_id'            : ['1', '2', '3', '4', '5'],
        'main_branch'              : ['I am a developer by profession',
                                      'I am a student', None,
                                      'I am a developer by profession',
                                      'I code primarily as a hobby'],
        'hobbyist'                 : ['Yes', 'No', 'Yes', None, 'Yes'],
        'age'                      : ['25-34 years old', '18-24 years old',
                                      None, '35-44 years old', '25-34 years old'],
        'gender'                   : ['Man', 'Woman', 'Man;Non-binary',
                                      None, 'Man'],
        'country'                  : ['United States', 'Nigeria', 'Germany',
                                      'Cameroon', 'India'],
        'ethnicity'                : ['White', 'Black', None, None, 'Asian'],
        'ed_level'                 : ["Bachelor's degree", "Master's degree",
                                      None, "Bachelor's degree", "Some college"],
        'undergraduate_major'      : ['Computer Science', 'Information Systems',
                                      None, 'Computer Science', 'Engineering'],
        'employment'               : ['Employed full-time', 'Student',
                                      'Employed full-time', 'Employed part-time',
                                      'Employed full-time'],
        'org_size'                 : ['100 to 499 employees', None,
                                      'Just me', None, '1,000 to 4,999 employees'],
        'dev_type'                 : ['Back-end developer;Full-stack developer',
                                      'Student', 'Front-end developer',
                                      'DBA/sysadmin', 'Data scientist'],
        'years_code'               : ['5', '2', 'Less than 1 year',
                                      'More than 50 years', '10'],
        'years_code_pro'           : ['3', None, '0', '30', '7'],
        'currency'                 : ['USD', 'NGN', 'EUR', 'XAF', 'INR'],
        'comp_total'               : ['95000', '500000', '75000',
                                      None, '1200000'],
        'comp_freq'                : ['Yearly', 'Monthly', 'Yearly',
                                      None, 'Yearly'],
        'language_worked_with'     : ['Python;SQL;JavaScript', 'Python;Java',
                                      'JavaScript;TypeScript', 'SQL;T-SQL',
                                      'Python;R;SQL'],
        'language_desired_next_year': ['Python;Rust', 'Python;Go',
                                       'Python', 'Python;SQL', 'Julia;Python'],
        'database_worked_with'     : ['MySQL;PostgreSQL', 'MySQL',
                                      'MongoDB', 'SQL Server', 'MySQL;Redis'],
        'database_desired_next_year': ['PostgreSQL', 'PostgreSQL',
                                       'MongoDB', 'MySQL', 'PostgreSQL'],
        'platform_worked_with'     : ['AWS', 'Azure', 'AWS;GCP',
                                      'Azure', 'AWS'],
        'platform_desired_next_year': ['AWS', 'AWS', 'AWS', 'Azure', 'GCP'],
        'web_frame_worked_with'    : ['React;Django', 'Spring', 'React;Vue.js',
                                      None, 'Django;Flask'],
        'web_frame_desired_next_year': ['FastAPI', 'React', 'Vue.js',
                                        None, 'FastAPI'],
        'job_sat'                  : ['Very satisfied', 'Slightly satisfied',
                                      'Neither satisfied nor dissatisfied',
                                      'Slightly dissatisfied', 'Very satisfied'],
        'career_sat'               : ['Very satisfied', 'Slightly satisfied',
                                      'Very satisfied', None, 'Very satisfied'],
        'work_week_hrs'            : ['40', '20', '45', '50', '38'],
        'op_sys'                   : ['Windows', 'Linux-based', 'MacOS',
                                      'Windows', 'Linux-based'],
        'survey_year'              : [2020, 2020, 2020, 2020, 2020],
    })


# ============================================
# EXTRACT TESTS
# ============================================

class TestExtract:

    def test_validate_data_passes_with_good_data(self, sample_raw_df):
        """Valid DataFrame should pass validation."""
        import os
        os.makedirs('logs', exist_ok=True)
        from extract import validate_extracted_data
        assert validate_extracted_data(sample_raw_df) == True

    def test_validate_data_fails_with_empty_df(self):
        """Empty DataFrame should fail validation."""
        import os
        os.makedirs('logs', exist_ok=True)
        from extract import validate_extracted_data
        empty_df = pd.DataFrame()
        assert validate_extracted_data(empty_df) == False

    def test_validate_data_fails_with_missing_columns(self):
        """DataFrame missing key columns should fail."""
        import os
        os.makedirs('logs', exist_ok=True)
        from extract import validate_extracted_data
        bad_df = pd.DataFrame({'random_col': [1, 2, 3]})
        assert validate_extracted_data(bad_df) == False

    def test_validate_data_has_required_columns(self, sample_raw_df):
        """DataFrame must have all required columns."""
        required = ['Respondent', 'MainBranch', 'Hobbyist', 'Country']
        for col in required:
            assert col in sample_raw_df.columns

    def test_check_file_exists_returns_false_for_missing_file(self):
        """Should return False for non-existent file."""
        import os
        os.makedirs('logs', exist_ok=True)
        from extract import check_file_exists
        assert check_file_exists('non_existent_file.csv') == False


# ============================================
# TRANSFORM TESTS
# ============================================

class TestTransform:

    def test_clean_years_coding_less_than_1(self):
        """'Less than 1 year' should return 0."""
        from transform import clean_years_coding
        assert clean_years_coding('Less than 1 year') == 0

    def test_clean_years_coding_more_than_50(self):
        """'More than 50 years' should return 51."""
        from transform import clean_years_coding
        assert clean_years_coding('More than 50 years') == 51

    def test_clean_years_coding_numeric_string(self):
        """Numeric string should convert to int."""
        from transform import clean_years_coding
        assert clean_years_coding('10') == 10

    def test_clean_years_coding_handles_null(self):
        """None value should return None."""
        from transform import clean_years_coding
        assert clean_years_coding(None) is None

    def test_clean_respondents_returns_dataframe(self, sample_raw_db_df):
        """clean_respondents should return a DataFrame."""
        from transform import clean_respondents
        result = clean_respondents(sample_raw_db_df)
        assert isinstance(result, pd.DataFrame)

    def test_clean_respondents_has_correct_columns(self, sample_raw_db_df):
        """Cleaned respondents should have expected columns."""
        from transform import clean_respondents
        result = clean_respondents(sample_raw_db_df)
        expected_cols = [
            'respondent_id', 'country',
            'employment_status', 'survey_year'
        ]
        for col in expected_cols:
            assert col in result.columns

    def test_clean_respondents_drops_null_ids(self, sample_raw_db_df):
        """Rows with null respondent_id should be dropped."""
        from transform import clean_respondents
        # Add a row with null respondent_id
        bad_row = sample_raw_db_df.copy()
        bad_row.loc[5] = [None] + [None] * (len(bad_row.columns) - 1)
        result = clean_respondents(bad_row)
        assert result['respondent_id'].isna().sum() == 0

    def test_hobbyist_converts_to_boolean(self, sample_raw_db_df):
        """Hobbyist Yes/No should become True/False."""
        from transform import clean_respondents
        result = clean_respondents(sample_raw_db_df)
        valid_values = [True, False, None, pd.NA]
        for val in result['is_hobbyist'].dropna():
            assert val in [True, False]

    def test_gender_takes_first_value(self, sample_raw_db_df):
        """Gender with multiple values should take first one."""
        from transform import clean_respondents
        result = clean_respondents(sample_raw_db_df)
        # Row 3 has 'Man;Non-binary' - should become 'Man'
        assert ';' not in str(result.iloc[2]['gender'])

    def test_clean_technologies_splits_semicolons(self, sample_raw_db_df):
        """Technologies should be split into individual rows."""
        from transform import clean_technologies
        result = clean_technologies(sample_raw_db_df)
        # Should have MORE rows than original (because of splitting)
        assert len(result) > len(sample_raw_db_df)

    def test_clean_technologies_no_semicolons_in_tech_name(self, sample_raw_db_df):
        """No tech_name should contain semicolons after splitting."""
        from transform import clean_technologies
        result = clean_technologies(sample_raw_db_df)
        assert not result['tech_name'].str.contains(';').any()

    def test_clean_compensation_removes_unrealistic_values(self, sample_raw_db_df):
        """Salaries below 100 or above 10M should be removed."""
        from transform import clean_compensation
        # Add unrealistic salary
        bad_df = sample_raw_db_df.copy()
        bad_df.loc[5] = bad_df.iloc[0].copy()
        bad_df.loc[5, 'respondent_id'] = '99'
        bad_df.loc[5, 'comp_total']    = '5'  # Unrealistically low
        result = clean_compensation(bad_df)
        valid = result[result['comp_total'].notna()]
        assert (valid['comp_total'] >= 100).all()

    def test_clean_dev_types_splits_semicolons(self, sample_raw_db_df):
        """Dev types should be split into individual rows."""
        from transform import clean_dev_types
        result = clean_dev_types(sample_raw_db_df)
        assert len(result) > 0
        assert ';' not in str(result['dev_type'].values)

    def test_build_dim_geography_has_continent(self, sample_raw_db_df):
        """Geography dimension should have continent column."""
        from transform import clean_respondents, build_dim_geography
        respondents = clean_respondents(sample_raw_db_df)
        result      = build_dim_geography(respondents)
        assert 'continent' in result.columns
        assert 'region' in result.columns

    def test_build_fact_table_has_satisfaction_scores(self, sample_raw_db_df):
        """Fact table should convert satisfaction text to scores."""
        from transform import (
            clean_respondents,
            clean_compensation,
            build_fact_table
        )
        respondents  = clean_respondents(sample_raw_db_df)
        compensation = clean_compensation(sample_raw_db_df)
        fact         = build_fact_table(respondents, compensation)
        assert 'job_satisfaction_score' in fact.columns
        # Scores should be 1-5 or null
        valid_scores = fact['job_satisfaction_score'].dropna()
        assert valid_scores.between(1, 5).all()


# ============================================
# LOAD TESTS (Unit tests - no DB needed)
# ============================================

class TestLoad:

    def test_prepare_dataframe_maps_columns(self, sample_raw_df):
        """prepare_dataframe should rename CSV columns to DB columns."""
        from load import prepare_dataframe
        result = prepare_dataframe(sample_raw_df)
        assert 'respondent_id' in result.columns
        assert 'country' in result.columns
        assert 'survey_year' in result.columns

    def test_prepare_dataframe_adds_survey_year(self, sample_raw_df):
        """prepare_dataframe should add survey_year = 2020."""
        from load import prepare_dataframe
        result = prepare_dataframe(sample_raw_df)
        assert (result['survey_year'] == 2020).all()

    def test_prepare_dataframe_no_original_column_names(self, sample_raw_df):
        """Original CSV column names should not exist after mapping."""
        from load import prepare_dataframe
        result = prepare_dataframe(sample_raw_df)
        # These are CSV names that should be renamed
        assert 'Respondent' not in result.columns
        assert 'MainBranch' not in result.columns


# ============================================
# DATA QUALITY TESTS
# ============================================

class TestDataQuality:

    def test_no_duplicate_respondent_ids(self, sample_raw_db_df):
        """Each respondent_id should appear only once."""
        from transform import clean_respondents
        result = clean_respondents(sample_raw_db_df)
        assert result['respondent_id'].duplicated().sum() == 0

    def test_survey_year_is_always_2020(self, sample_raw_db_df):
        """All records should have survey_year = 2020."""
        from transform import clean_respondents
        result = clean_respondents(sample_raw_db_df)
        assert (result['survey_year'] == 2020).all()

    def test_work_week_hrs_is_numeric(self, sample_raw_db_df):
        """Work week hours should be numeric after cleaning."""
        from transform import clean_respondents
        result = clean_respondents(sample_raw_db_df)
        numeric_hrs = pd.to_numeric(
            result['work_week_hrs'], errors='coerce'
        )
        # Non-null values should all be numeric
        assert numeric_hrs.notna().sum() > 0

    def test_countries_are_strings(self, sample_raw_db_df):
        """Country field should contain only strings."""
        from transform import clean_respondents
        result = clean_respondents(sample_raw_db_df)
        non_null_countries = result['country'].dropna()
        assert all(isinstance(c, str) for c in non_null_countries)
# ```

# ---

# ### 📖 What These Tests Do:
# ```
# TestExtract
# ├── Valid data passes validation ✅
# ├── Empty DataFrame fails validation ✅
# ├── Missing columns fails validation ✅
# └── Missing file returns False ✅

# TestTransform
# ├── Years coding converts correctly ✅
# ├── Nulls handled properly ✅
# ├── Gender splits on semicolons ✅
# ├── Technologies split into rows ✅
# ├── Unrealistic salaries removed ✅
# └── Satisfaction converts to scores ✅

# TestLoad
# ├── Column mapping works correctly ✅
# ├── Survey year added as 2020 ✅
# └── Original column names removed ✅

# TestDataQuality
# ├── No duplicate respondent IDs ✅
# ├── Survey year always 2020 ✅
# ├── Work hours are numeric ✅
# └── Countries are strings ✅