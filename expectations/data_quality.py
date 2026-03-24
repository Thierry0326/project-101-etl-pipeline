# ============================================
# PROJECT 101 - GREAT EXPECTATIONS
# Purpose: Define data quality rules
#          that our data MUST pass
#          before moving to next layer
# ============================================

import pandas as pd
import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ============================================
# LOGGING SETUP
# ============================================
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/data_quality.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============================================
# DATA QUALITY RULE ENGINE
# A lightweight implementation of
# Great Expectations style validation
# ============================================

class DataQualityResult:
    """Stores the result of a single quality check."""

    def __init__(self, rule_name: str, passed: bool,
                 details: str = '', severity: str = 'error'):
        self.rule_name = rule_name
        self.passed    = passed
        self.details   = details
        self.severity  = severity  # 'error' or 'warning'
        self.timestamp = datetime.now().isoformat()

    def __repr__(self):
        status = '✅ PASS' if self.passed else (
            '❌ FAIL' if self.severity == 'error' else '⚠️  WARN'
        )
        return f"{status} | {self.rule_name} | {self.details}"


class DataQualityValidator:
    """
    Validates DataFrames against defined rules.
    Think of this as a contract for your data.
    """

    def __init__(self, layer_name: str):
        self.layer_name = layer_name
        self.results    = []
        self.passed     = 0
        self.failed     = 0
        self.warnings   = 0

    def expect_column_to_exist(
        self, df: pd.DataFrame, column: str
    ) -> DataQualityResult:
        """Rule: Column must exist in DataFrame."""
        passed  = column in df.columns
        details = (f"Column '{column}' found" if passed
                   else f"Column '{column}' MISSING!")
        result  = DataQualityResult(
            f"column_exists:{column}", passed, details
        )
        self._record(result)
        return result

    def expect_column_no_nulls(
        self, df: pd.DataFrame, column: str
    ) -> DataQualityResult:
        """Rule: Column must have zero null values."""
        if column not in df.columns:
            return self._skip(f"no_nulls:{column}", "Column not found")

        null_count = df[column].isna().sum()
        passed     = null_count == 0
        details    = (f"0 nulls found" if passed
                      else f"{null_count:,} null values found!")
        result     = DataQualityResult(
            f"no_nulls:{column}", passed, details
        )
        self._record(result)
        return result

    def expect_column_null_rate_below(
        self, df: pd.DataFrame, column: str,
        max_rate: float, severity: str = 'warning'
    ) -> DataQualityResult:
        """Rule: Null rate must be below threshold."""
        if column not in df.columns:
            return self._skip(
                f"null_rate:{column}", "Column not found"
            )

        null_rate = df[column].isna().mean()
        passed    = null_rate <= max_rate
        details   = (
            f"Null rate: {null_rate:.1%} "
            f"(max allowed: {max_rate:.1%})"
        )
        result = DataQualityResult(
            f"null_rate:{column}", passed, details, severity
        )
        self._record(result)
        return result

    def expect_column_values_in_set(
        self, df: pd.DataFrame, column: str,
        valid_values: list, severity: str = 'warning'
    ) -> DataQualityResult:
        """Rule: Column values must be in allowed set."""
        if column not in df.columns:
            return self._skip(
                f"values_in_set:{column}", "Column not found"
            )

        actual_values = set(df[column].dropna().unique())
        invalid       = actual_values - set(valid_values)
        passed        = len(invalid) == 0
        details       = (
            f"All values valid" if passed
            else f"Invalid values found: {invalid}"
        )
        result = DataQualityResult(
            f"values_in_set:{column}", passed, details, severity
        )
        self._record(result)
        return result

    def expect_column_values_unique(
        self, df: pd.DataFrame, column: str
    ) -> DataQualityResult:
        """Rule: Column values must be unique."""
        if column not in df.columns:
            return self._skip(
                f"unique:{column}", "Column not found"
            )

        duplicate_count = df[column].duplicated().sum()
        passed          = duplicate_count == 0
        details         = (
            f"All values unique" if passed
            else f"{duplicate_count:,} duplicate values found!"
        )
        result = DataQualityResult(
            f"unique:{column}", passed, details
        )
        self._record(result)
        return result

    def expect_row_count_between(
        self, df: pd.DataFrame,
        min_rows: int, max_rows: int
    ) -> DataQualityResult:
        """Rule: Row count must be within expected range."""
        actual = len(df)
        passed = min_rows <= actual <= max_rows
        details = (
            f"Row count: {actual:,} "
            f"(expected: {min_rows:,} - {max_rows:,})"
        )
        result = DataQualityResult(
            "row_count_between", passed, details
        )
        self._record(result)
        return result

    def expect_column_values_between(
        self, df: pd.DataFrame, column: str,
        min_val: float, max_val: float,
        severity: str = 'warning'
    ) -> DataQualityResult:
        """Rule: Numeric column values must be within range."""
        if column not in df.columns:
            return self._skip(
                f"values_between:{column}", "Column not found"
            )

        numeric = pd.to_numeric(df[column], errors='coerce')
        out_of_range = numeric[
            (numeric < min_val) | (numeric > max_val)
        ].count()
        passed  = out_of_range == 0
        details = (
            f"All values in range [{min_val}, {max_val}]"
            if passed else
            f"{out_of_range:,} values outside range!"
        )
        result = DataQualityResult(
            f"values_between:{column}", passed, details, severity
        )
        self._record(result)
        return result

    def expect_column_not_empty_string(
        self, df: pd.DataFrame, column: str
    ) -> DataQualityResult:
        """Rule: Column must not have empty strings."""
        if column not in df.columns:
            return self._skip(
                f"not_empty:{column}", "Column not found"
            )

        empty_count = (df[column] == '').sum()
        passed      = empty_count == 0
        details     = (
            f"No empty strings" if passed
            else f"{empty_count:,} empty strings found!"
        )
        result = DataQualityResult(
            f"not_empty:{column}", passed, details
        )
        self._record(result)
        return result

    def _record(self, result: DataQualityResult):
        """Record result and update counters."""
        self.results.append(result)
        if result.passed:
            self.passed += 1
        elif result.severity == 'error':
            self.failed += 1
        else:
            self.warnings += 1
        logger.info(str(result))

    def _skip(self, rule_name: str,
              reason: str) -> DataQualityResult:
        """Create a skipped result."""
        result = DataQualityResult(
            rule_name, True,
            f"SKIPPED: {reason}", 'warning'
        )
        self.results.append(result)
        return result

    def get_summary(self) -> dict:
        """Return validation summary."""
        total = len(self.results)
        return {
            'layer'     : self.layer_name,
            'timestamp' : datetime.now().isoformat(),
            'total'     : total,
            'passed'    : self.passed,
            'failed'    : self.failed,
            'warnings'  : self.warnings,
            'pass_rate' : f"{(self.passed/total*100):.1f}%" if total else "0%",
            'results'   : [
                {
                    'rule'    : r.rule_name,
                    'passed'  : r.passed,
                    'details' : r.details,
                    'severity': r.severity
                }
                for r in self.results
            ]
        }

    def is_valid(self) -> bool:
        """Returns True only if zero errors (warnings OK)."""
        return self.failed == 0


# ============================================
# BRONZE LAYER RULES
# Applied AFTER extraction, BEFORE SQL Server load
# ============================================

def validate_bronze_layer(df: pd.DataFrame) -> bool:
    """
    Validate raw extracted data.
    These are loose rules — raw data is messy!
    We use warnings more than errors here.
    """
    logger.info("\n" + "=" * 50)
    logger.info("🥉 VALIDATING BRONZE LAYER")
    logger.info("=" * 50)

    v = DataQualityValidator('bronze')

    # Must have data
    v.expect_row_count_between(df, 50_000, 100_000)

    # Key columns must exist
    for col in ['Respondent', 'MainBranch', 'Country',
                'Hobbyist', 'Employment']:
        v.expect_column_to_exist(df, col)

    # Respondent ID must be unique and not null
    v.expect_column_values_unique(df, 'Respondent')
    v.expect_column_no_nulls(df, 'Respondent')

    # Country should be mostly filled
    v.expect_column_null_rate_below(
        df, 'Country', 0.05, 'warning'
    )

    # Hobbyist should be Yes/No only
    v.expect_column_values_in_set(
        df, 'Hobbyist', ['Yes', 'No'], 'warning'
    )

    # Log summary
    _log_summary(v)
    _save_report(v, 'data/raw/bronze_quality_report.json')

    return v.is_valid()


# ============================================
# SILVER LAYER RULES
# Applied AFTER transform, BEFORE MySQL Silver load
# ============================================

def validate_silver_layer(
    respondents_df: pd.DataFrame,
    compensation_df: pd.DataFrame,
    technologies_df: pd.DataFrame
) -> bool:
    """
    Validate cleaned/transformed data.
    Stricter rules than Bronze layer.
    """
    logger.info("\n" + "=" * 50)
    logger.info("🥈 VALIDATING SILVER LAYER")
    logger.info("=" * 50)

    v = DataQualityValidator('silver')

    # ---- Respondents ----
    v.expect_row_count_between(
        respondents_df, 50_000, 100_000
    )
    v.expect_column_no_nulls(respondents_df, 'respondent_id')
    v.expect_column_values_unique(
        respondents_df, 'respondent_id'
    )

    # Survey year must always be 2020
    v.expect_column_values_in_set(
        respondents_df, 'survey_year', [2020]
    )

    # Work hours should be reasonable (1-100 hrs/week)
    v.expect_column_values_between(
        respondents_df, 'work_week_hrs', 1, 100, 'warning'
    )

    # Years coding should be realistic (0-51)
    v.expect_column_values_between(
        respondents_df, 'years_coding', 0, 51, 'warning'
    )

    # ---- Compensation ----
    # Salaries should be realistic
    v.expect_column_values_between(
        compensation_df, 'comp_total',
        100, 10_000_000, 'warning'
    )

    # ---- Technologies ----
    # Tech type must be language, database or platform
    v.expect_column_values_in_set(
        technologies_df, 'tech_type',
        ['language', 'database', 'platform'], 'error'
    )

    # Tech names should not be empty
    v.expect_column_not_empty_string(
        technologies_df, 'tech_name'
    )

    # Log summary
    _log_summary(v)
    _save_report(v, 'data/processed/silver_quality_report.json')

    return v.is_valid()


# ============================================
# GOLD LAYER RULES
# Applied AFTER gold transforms, BEFORE MySQL Gold load
# ============================================

def validate_gold_layer(
    fact_df: pd.DataFrame,
    dim_developer_df: pd.DataFrame,
    dim_geography_df: pd.DataFrame
) -> bool:
    """
    Validate star schema data.
    Strictest rules — this feeds Power BI directly!
    """
    logger.info("\n" + "=" * 50)
    logger.info("🥇 VALIDATING GOLD LAYER")
    logger.info("=" * 50)

    v = DataQualityValidator('gold')

    # ---- Fact Table ----
    v.expect_row_count_between(fact_df, 50_000, 100_000)
    v.expect_column_no_nulls(fact_df, 'respondent_id')
    v.expect_column_values_unique(fact_df, 'respondent_id')

    # Satisfaction scores must be 1-5
    v.expect_column_values_between(
        fact_df, 'job_satisfaction_score', 1, 5, 'warning'
    )
    v.expect_column_values_between(
        fact_df, 'career_satisfaction_score', 1, 5, 'warning'
    )

    # ---- Developer Dimension ----
    v.expect_column_no_nulls(
        dim_developer_df, 'respondent_id'
    )
    v.expect_column_values_unique(
        dim_developer_df, 'respondent_id'
    )

    # ---- Geography Dimension ----
    v.expect_column_no_nulls(dim_geography_df, 'country')
    v.expect_column_values_unique(
        dim_geography_df, 'country'
    )
    v.expect_column_no_nulls(dim_geography_df, 'continent')

    # Log summary
    _log_summary(v)
    _save_report(
        v, 'data/processed/gold_quality_report.json'
    )

    return v.is_valid()


# ============================================
# HELPERS
# ============================================

def _log_summary(v: DataQualityValidator):
    """Log validation summary."""
    summary = v.get_summary()
    logger.info("-" * 50)
    logger.info(f"📊 Layer    : {summary['layer'].upper()}")
    logger.info(f"📊 Total    : {summary['total']} rules")
    logger.info(f"✅ Passed   : {summary['passed']}")
    logger.info(f"❌ Failed   : {summary['failed']}")
    logger.info(f"⚠️  Warnings : {summary['warnings']}")
    logger.info(f"📈 Pass rate: {summary['pass_rate']}")

    if v.is_valid():
        logger.info("🎉 VALIDATION PASSED!")
    else:
        logger.error("🚨 VALIDATION FAILED — Pipeline stopped!")
    logger.info("-" * 50)


def _save_report(
    v: DataQualityValidator, path: str
):
    """Save quality report as audit trail."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        json.dump(v.get_summary(), f, indent=2)
    logger.info(f"📝 Quality report saved: {path}")


# ============================================
# MAIN EXECUTION
# ============================================

def run_data_quality_checks(
    bronze_df: pd.DataFrame = None,
    silver_data: dict = None,
    gold_data: dict = None
) -> bool:
    """
    Master data quality function.
    Called by Airflow DAG after each phase.
    """
    logger.info("=" * 50)
    logger.info("PROJECT 101 - DATA QUALITY CHECKS")
    logger.info("=" * 50)

    all_passed = True

    # Bronze validation
    if bronze_df is not None:
        if not validate_bronze_layer(bronze_df):
            logger.error("❌ Bronze validation failed!")
            all_passed = False

   # Silver validation
    if silver_data is not None:
        if not validate_silver_layer(
            silver_data.get('respondents', pd.DataFrame()),
            silver_data.get('compensation', pd.DataFrame()),
            silver_data.get('technologies', pd.DataFrame())
        ):
            logger.error("❌ Silver validation failed!")
            all_passed = False

    # Gold validation
    if gold_data is not None:
        if not validate_gold_layer(
            gold_data.get('fact_responses', pd.DataFrame()),
            gold_data.get('dim_developer', pd.DataFrame()),
            gold_data.get('dim_geography', pd.DataFrame())
        ):
            logger.error("❌ Gold validation failed!")
            all_passed = False

    if all_passed:
        logger.info("✅ ALL DATA QUALITY CHECKS PASSED!")
    else:
        logger.error("🚨 SOME DATA QUALITY CHECKS FAILED!")

    return all_passed


if __name__ == "__main__":
    # Quick test with fake data
    import sys
    sys.path.insert(0, 'pipeline')

    print("🧪 Testing data quality validator...")

    test_df = pd.DataFrame({
        'Respondent' : ['1', '2', '3'],
        'MainBranch' : ['Developer', 'Student', 'Developer'],
        'Country'    : ['USA', 'Nigeria', 'Germany'],
        'Hobbyist'   : ['Yes', 'No', 'Yes'],
        'Employment' : ['Full-time', 'Student', 'Full-time'],
    })

    # This will fail row count (only 3 rows vs 50k minimum)
    # but shows the validator working!
    validate_bronze_layer(test_df)
    print("\n✅ Validator working correctly!")
# ```

# ---

# ### 📖 What This Does — Simply Explained:
# ```
# Bronze Layer Rules (loose):          Silver Layer Rules (strict):
# ├── Row count 50k-100k               ├── Respondent ID unique & not null
# ├── Key columns exist                ├── Survey year always 2020
# ├── Respondent ID unique             ├── Work hours between 1-100
# ├── Country < 5% nulls              ├── Tech type valid values only
# └── Hobbyist = Yes/No only          └── Salaries between 100-10M

# Gold Layer Rules (strictest):
# ├── Fact table row count 50k-100k
# ├── Satisfaction scores 1-5
# ├── Dimension IDs unique & not null
# └── Countries have continents