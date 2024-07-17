from datetime import datetime
from functools import reduce

import pandas as pd
import pytest

import calista.core.functions as F
import calista.core.rules as R
from calista.core.metrics import Metrics
from calista.table import CalistaTable


class TestPandasTable:

    expected_dataset_row_count = 100

    def test_is_date(self, pandas_table):
        dates_rule_name = "check_dates"
        date_columns = [
            "DATE_ENTREE",
            "DATE_NAISSANCE",
            "DATE_SORTIE",
            "DATE_DERNIER_EA",
            "DATE_DERNIERE_AUGMENTATION",
        ]
        date_rules = [F.is_date(date_col) for date_col in date_columns]
        dates_rule = reduce(
            lambda agg_cond, curr_cond: agg_cond & curr_cond, date_rules
        )

        expected_valid_row_count = 57

        self.analyze_and_assert_rule(
            pandas_table, dates_rule_name, dates_rule, expected_valid_row_count
        )

    def test_is_iban(self, pandas_table):
        iban_rule_name = "check_iban_quality"
        iban_rule = F.is_iban("IBAN")

        expected_valid_row_count = 90

        self.analyze_and_assert_rule(
            pandas_table, iban_rule_name, iban_rule, expected_valid_row_count
        )

    def test_is_integer(self, pandas_table):
        integer_rule_name = "check_CDI_ID_are_integer"
        integer_rule = F.is_integer("CDI") & F.is_integer("ID")

        expected_valid_row_count = 98

        self.analyze_and_assert_rule(
            pandas_table, integer_rule_name, integer_rule, expected_valid_row_count
        )

    def test_is_email(self, pandas_table):
        email_rule_name = "check_email_quality"
        email_rule = F.is_email("EMAIL")

        expected_valid_row_count = 92

        self.analyze_and_assert_rule(
            pandas_table, email_rule_name, email_rule, expected_valid_row_count
        )

    def test_is_boolean(self, pandas_table):
        boolean_rule_name = "check_CDD_CDI_are_boolean"
        boolean_rule = F.is_boolean("CDD") & F.is_boolean("CDI")

        expected_valid_row_count = 88

        self.analyze_and_assert_rule(
            pandas_table, boolean_rule_name, boolean_rule, expected_valid_row_count
        )

    def test_is_unique(self, pandas_table):
        id_rule_name = "check_ID_is_unique"
        id_rule = F.is_unique("ID")

        expected_valid_row_count = 100

        self.analyze_and_assert_rule(
            pandas_table, id_rule_name, id_rule, expected_valid_row_count
        )

    def test_is_phone_number(self, pandas_table):
        phone_number_rule_name = "check_is_phone_number"
        phone_number_rule = F.is_phone_number("TELEPHONE")

        expected_valid_row_count = 80

        self.analyze_and_assert_rule(
            pandas_table,
            phone_number_rule_name,
            phone_number_rule,
            expected_valid_row_count,
        )

    def test_is_float(self, pandas_table):
        salary_rule_name = "check_salary_is_float"
        salary_rule = F.is_float("SALAIRE")

        expected_valid_row_count = 84

        self.analyze_and_assert_rule(
            pandas_table, salary_rule_name, salary_rule, expected_valid_row_count
        )

    def test_outliers_continuous_var(self, pandas_table):
        outlier_rule = R.GetOutliersForContinuousVar(col_name="SALAIRE")
        expected_outlier_values = []

        outlier_values = pandas_table._engine.get_outliers_for_continuous_var(
            outlier_rule
        )
        assert outlier_values == expected_outlier_values

    def test_outliers_discrete_var(self, pandas_table):
        count_occurences_rule = R.CountOccurences(col_name="DATE_ENTREE")
        expected_outlier_values = []

        outlier_values = pandas_table._engine.get_outliers_for_discrete_var(
            count_occurences_rule
        )
        assert outlier_values == expected_outlier_values

    def test_is_ip_address(self, pandas_table):
        ip_address_rule_name = "check_ip_address_quality"
        ip_address_rule = F.is_ip_address("ADRESSE_IP_V4") & F.is_ip_address(
            "ADRESSE_IP_V6"
        )

        expected_valid_row_count = 98

        self.analyze_and_assert_rule(
            pandas_table,
            ip_address_rule_name,
            ip_address_rule,
            expected_valid_row_count,
        )

    def test_compare_column_to_value(self, pandas_table):
        salary_rule_name = "check_secteur_activite_banque_finance"
        salary_rule = F.compare_column_to_value(
            "SECTEUR_ACTIVITE", operator="=", value="Banque/Finance"
        )

        expected_valid_row_count = 17

        self.analyze_and_assert_rule(
            pandas_table, salary_rule_name, salary_rule, expected_valid_row_count
        )

    def test_compare_column_to_column(self, pandas_table):
        salary_rule_name = "check_salaire_id"
        salary_rule = F.compare_column_to_column(
            col_left="SALAIRE", operator="=", col_right="ID"
        )

        expected_valid_row_count = 0

        self.analyze_and_assert_rule(
            pandas_table, salary_rule_name, salary_rule, expected_valid_row_count
        )

    def test_integer_digit(self, pandas_table):
        salary_rule_name = "check_salare_integer_3_digit"
        salary_rule = F.count_integer_digit("SALAIRE", operator="=", digit=3)

        expected_valid_row_count = 0

        self.analyze_and_assert_rule(
            pandas_table, salary_rule_name, salary_rule, expected_valid_row_count
        )

    def test_decimal_digit(self, pandas_table):
        salary_rule_name = "check_salare_decimal_3_digit"
        salary_rule = F.count_decimal_digit("SALAIRE", operator=">=", digit=3)

        expected_valid_row_count = 8

        self.analyze_and_assert_rule(
            pandas_table, salary_rule_name, salary_rule, expected_valid_row_count
        )

    def test_is_between(self, pandas_table):
        salary_rule_name = "check_salare_between_60000_80000"
        salary_rule = F.is_between("SALAIRE", min_value=60000, max_value=80000)

        expected_valid_row_count = 23

        self.analyze_and_assert_rule(
            pandas_table, salary_rule_name, salary_rule, expected_valid_row_count
        )

    def test_compare_length(self, pandas_table):
        salary_rule_name = "check_compare_secteur_activite"
        salary_rule = F.compare_length(
            col_name="SECTEUR_ACTIVITE", operator=">", length=8
        )

        expected_valid_row_count = 80

        self.analyze_and_assert_rule(
            pandas_table, salary_rule_name, salary_rule, expected_valid_row_count
        )

    def test_is_alphabetic(self, pandas_table):
        salary_rule_name = "check_nom_is_alphabetic"
        salary_rule = F.is_alphabetic(col_name="NOM")

        expected_valid_row_count = 96

        self.analyze_and_assert_rule(
            pandas_table, salary_rule_name, salary_rule, expected_valid_row_count
        )

    def test_is_negative(self, pandas_table):
        salary_rule_name = "check_salaire_is_not_negative"
        salary_rule = F.is_negative(col_name="SALAIRE")

        expected_valid_row_count = 0

        self.analyze_and_assert_rule(
            pandas_table, salary_rule_name, salary_rule, expected_valid_row_count
        )

    def test_is_positive(self, pandas_table):
        salary_rule_name = "check_salaire_is_positive"
        salary_rule = F.is_positive(col_name="SALAIRE")

        expected_valid_row_count = 87

        self.analyze_and_assert_rule(
            pandas_table, salary_rule_name, salary_rule, expected_valid_row_count
        )

    def test_not_condition(self, pandas_table):
        salary_rule_name = "check_Prenom_not_not_null"
        salary_rule = ~F.is_not_null(col_name="PRENOM")

        expected_valid_row_count = 12

        self.analyze_and_assert_rule(
            pandas_table, salary_rule_name, salary_rule, expected_valid_row_count
        )

    def analyze_and_assert_rule(
        self, pandas_table, rule_name, rule, expected_valid_row_count
    ):
        computed_metrics = pandas_table.analyze(rule_name, rule)
        expected_metrics = Metrics(
            rule=rule_name,
            total_row_count=self.expected_dataset_row_count,
            valid_row_count=expected_valid_row_count,
            valid_row_count_pct=expected_valid_row_count
            * 100
            / self.expected_dataset_row_count,
            timestamp=computed_metrics.timestamp,
        )

        assert computed_metrics == expected_metrics

    def test_analyze_rules(self, pandas_table):
        rules_with_expected_valid_count = {
            "check_iban_quality": (F.is_iban("IBAN"), 90),
            "check_CDI_ID_are_integer": (F.is_integer("CDI") & F.is_integer("ID"), 98),
            "check_email_quality": (F.is_email("EMAIL"), 92),
            "check_ID_unicity": (F.is_unique("ID"), 100),
        }
        rules = {
            rule_name: rule_with_valid_count[0]
            for rule_name, rule_with_valid_count in rules_with_expected_valid_count.items()
        }

        computed_metrics = pandas_table.analyze_rules(rules)

        metrics_timestamp = computed_metrics[0].timestamp
        expected_metrics = [
            Metrics(
                rule=rule_name,
                total_row_count=self.expected_dataset_row_count,
                valid_row_count=rule_with_valid_count[1],
                valid_row_count_pct=rule_with_valid_count[1]
                * 100
                / self.expected_dataset_row_count,
                timestamp=metrics_timestamp,
            )
            for rule_name, rule_with_valid_count in rules_with_expected_valid_count.items()
        ]

        assert computed_metrics == expected_metrics

    def test_pandas_table_from_dict(self):
        data_dict = {
            "integer": [1, 2, 3],
            "date": [
                datetime(2025, 1, 1),
                datetime(2025, 1, 2),
                datetime(2025, 1, 3),
            ],
            "float": [4.0, 5.0, 6.0],
            "string": ["a", "b", "c"],
        }
        calista_table_from_dict = CalistaTable("pandas").load_from_dict(data_dict)
        assert calista_table_from_dict._engine.dataset.equals(pd.DataFrame(data_dict))

    def groupby_and_assert_rule(
        self,
        pandas_table,
        keys,
        rule_name,
        rule,
        expected_valid_row_count,
        expected_dataset_row_count,
    ):
        computed_metrics = pandas_table.groupBy(keys).analyze(rule_name, rule)
        expected_metrics = Metrics(
            rule=rule_name,
            total_row_count=expected_dataset_row_count,
            valid_row_count=expected_valid_row_count,
            valid_row_count_pct=expected_valid_row_count
            * 100
            / expected_dataset_row_count,
            timestamp=computed_metrics.timestamp,
        )

        assert computed_metrics == expected_metrics

    def test_compare_sum_to_value(self, pandas_table):
        sum_by_gt_rule_name = "check_sum_salary_by_sex_gt"
        sum_by_gt_rule = F.sum_gt_value(col_name="SALAIRE", value=20000)

        expected_dataset_row_count = 2
        expected_valid_row_count = 2

        self.groupby_and_assert_rule(
            pandas_table,
            "SEXE",
            sum_by_gt_rule_name,
            sum_by_gt_rule,
            expected_valid_row_count,
            expected_dataset_row_count,
        )

    def test_compare_count_to_value(self, pandas_table):
        sum_by_lt_rule_name = "check_count_salary_by_sex_lt"
        sum_by_lt_rule = F.count_lt_value(col_name="SALAIRE", value=20000)

        expected_dataset_row_count = 2
        expected_valid_row_count = 2

        self.groupby_and_assert_rule(
            pandas_table,
            "SEXE",
            sum_by_lt_rule_name,
            sum_by_lt_rule,
            expected_valid_row_count,
            expected_dataset_row_count,
        )

    def test_compare_mean_to_value(self, pandas_table):
        sum_by_lt_rule_name = "check_mean_salary_by_sex_lt"
        sum_by_lt_rule = F.mean_lt_value(col_name="SALAIRE", value=20000)

        expected_dataset_row_count = 2
        expected_valid_row_count = 0

        self.groupby_and_assert_rule(
            pandas_table,
            "SEXE",
            sum_by_lt_rule_name,
            sum_by_lt_rule,
            expected_valid_row_count,
            expected_dataset_row_count,
        )

    def test_compare_median_to_value(self, pandas_table):
        median_by_lt_rule_name = "check_median_salary_by_sex_lt"
        median_by_lt_rule = F.median_lt_value(col_name="SALAIRE", value=20000)

        expected_dataset_row_count = 2
        expected_valid_row_count = 0

        self.groupby_and_assert_rule(
            pandas_table,
            "SEXE",
            median_by_lt_rule_name,
            median_by_lt_rule,
            expected_valid_row_count,
            expected_dataset_row_count,
        )

    def test_compare_min_to_value(self, pandas_table):
        min_by_lt_rule_name = "check_min_salary_by_sex_lt"
        min_by_lt_rule = F.min_lt_value(col_name="SALAIRE", value=33000)

        expected_dataset_row_count = 2
        expected_valid_row_count = 1

        self.groupby_and_assert_rule(
            pandas_table,
            "SEXE",
            min_by_lt_rule_name,
            min_by_lt_rule,
            expected_valid_row_count,
            expected_dataset_row_count,
        )

    def test_compare_max_to_value(self, pandas_table):
        max_by_lt_rule_name = "check_max_salary_by_sex_lt"
        max_by_lt_rule = F.max_lt_value(col_name="SALAIRE", value=500000)

        expected_dataset_row_count = 2
        expected_valid_row_count = 2

        self.groupby_and_assert_rule(
            pandas_table,
            "SEXE",
            max_by_lt_rule_name,
            max_by_lt_rule,
            expected_valid_row_count,
            expected_dataset_row_count,
        )

    def test_multiple_agg(self, pandas_table):
        rule_name = "check_sum_count_mean_salary_by_gender"
        rule = (
            F.sum_lt_value(col_name="SALAIRE", value=20000000)
            & F.count_le_value(col_name="SALAIRE", value=40)
            & F.mean_ge_value(col_name="SALAIRE", value=50000)
        )

        expected_dataset_row_count = 2
        expected_valid_row_count = 1

        self.groupby_and_assert_rule(
            pandas_table,
            "SEXE",
            rule_name,
            rule,
            expected_valid_row_count,
            expected_dataset_row_count,
        )

    def test_agg_and_normal_cond_combination(self, pandas_table):
        with pytest.raises(Exception) as combination_exception:
            F.sum_gt_value(col_name="SALAIRE", value=20000) | F.is_iban("SALAIRE")

        assert "Cannot combine Condition with AggregateCondition" == str(
            combination_exception.value
        )
