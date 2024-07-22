from datetime import datetime
from functools import reduce

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import calista.core.functions as F
import calista.core.rules as R
from calista.core.metrics import Metrics
from calista.table import CalistaEngine


class TestPolarsTable:

    expected_dataset_row_count = 100

    def test_is_date(self, polars_table):
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
            polars_table, dates_rule_name, dates_rule, expected_valid_row_count
        )

    def test_is_iban(self, polars_table):
        iban_rule_name = "check_iban_quality"
        iban_rule = F.is_iban("IBAN")

        expected_valid_row_count = 90

        self.analyze_and_assert_rule(
            polars_table, iban_rule_name, iban_rule, expected_valid_row_count
        )

    def test_is_integer(self, polars_table):
        integer_rule_name = "check_CDI_ID_are_integer"
        integer_rule = F.is_integer("CDI") & F.is_integer("ID")

        expected_valid_row_count = 98

        self.analyze_and_assert_rule(
            polars_table, integer_rule_name, integer_rule, expected_valid_row_count
        )

    def test_is_email(self, polars_table):
        email_rule_name = "check_email_quality"
        email_rule = F.is_email("EMAIL")

        expected_valid_row_count = 92

        self.analyze_and_assert_rule(
            polars_table, email_rule_name, email_rule, expected_valid_row_count
        )

    def test_is_boolean(self, polars_table):
        boolean_rule_name = "check_CDD_CDI_are_boolean"
        boolean_rule = F.is_boolean("CDD") & F.is_boolean("CDI")

        expected_valid_row_count = 88

        self.analyze_and_assert_rule(
            polars_table, boolean_rule_name, boolean_rule, expected_valid_row_count
        )

    def test_is_unique(self, polars_table):
        id_rule_name = "check_ID_is_unique"
        id_rule = F.is_unique("ID")

        expected_valid_row_count = 100

        self.analyze_and_assert_rule(
            polars_table, id_rule_name, id_rule, expected_valid_row_count
        )

    def test_is_phone_number(self, polars_table):
        phone_number_rule_name = "check_is_phone_number"
        phone_number_rule = F.is_phone_number("TELEPHONE")

        expected_valid_row_count = 80

        self.analyze_and_assert_rule(
            polars_table,
            phone_number_rule_name,
            phone_number_rule,
            expected_valid_row_count,
        )

    def test_is_float(self, polars_table):
        salary_rule_name = "check_salary_is_float"
        salary_rule = F.is_float("SALAIRE")

        expected_valid_row_count = 84

        self.analyze_and_assert_rule(
            polars_table, salary_rule_name, salary_rule, expected_valid_row_count
        )

    def test_outliers_continuous_var(self, polars_table):
        outlier_rule = R.GetOutliersForContinuousVar(col_name="SALAIRE")
        expected_outlier_values = []

        outlier_values = polars_table._engine.get_outliers_for_continuous_var(
            outlier_rule
        )
        assert outlier_values == expected_outlier_values

    def test_outliers_discrete_var(self, polars_table):
        count_occurences_rule = R.CountOccurences(col_name="DATE_ENTREE")
        expected_outlier_values = []

        outlier_values = polars_table._engine.get_outliers_for_discrete_var(
            count_occurences_rule
        )
        assert outlier_values == expected_outlier_values

    def test_is_ip_address(self, polars_table):
        ip_address_rule_name = "check_ip_address_quality"
        ip_address_rule = F.is_ip_address("ADRESSE_IP_V4") & F.is_ip_address(
            "ADRESSE_IP_V6"
        )

        expected_valid_row_count = 98

        self.analyze_and_assert_rule(
            polars_table,
            ip_address_rule_name,
            ip_address_rule,
            expected_valid_row_count,
        )

    def test_compare_column_to_value(self, polars_table):
        salary_rule_name = "check_secteur_activite_banque_finance"
        salary_rule = F.compare_column_to_value(
            "SECTEUR_ACTIVITE", operator="=", value="Banque/Finance"
        )

        expected_valid_row_count = 17

        self.analyze_and_assert_rule(
            polars_table, salary_rule_name, salary_rule, expected_valid_row_count
        )

    def test_compare_column_to_column(self, polars_table):
        salary_rule_name = "check_salaire_id"
        salary_rule = F.compare_column_to_column(
            col_left="SALAIRE", operator="=", col_right="ID"
        )

        expected_valid_row_count = 0

        self.analyze_and_assert_rule(
            polars_table, salary_rule_name, salary_rule, expected_valid_row_count
        )

    def test_integer_digit(self, polars_table):
        salary_rule_name = "check_salare_integer_3_digit"
        salary_rule = F.count_integer_digit("SALAIRE", operator="=", digit=3)

        expected_valid_row_count = 0

        self.analyze_and_assert_rule(
            polars_table, salary_rule_name, salary_rule, expected_valid_row_count
        )

    def test_decimal_digit(self, polars_table):
        salary_rule_name = "check_salare_decimal_3_digit"
        salary_rule = F.count_decimal_digit("SALAIRE", operator=">=", digit=3)

        expected_valid_row_count = 8

        self.analyze_and_assert_rule(
            polars_table, salary_rule_name, salary_rule, expected_valid_row_count
        )

    def test_is_between(self, polars_table):
        salary_rule_name = "check_salare_between_60000_80000"
        salary_rule = F.is_between("SALAIRE", min_value=60000, max_value=80000)

        expected_valid_row_count = 23

        self.analyze_and_assert_rule(
            polars_table, salary_rule_name, salary_rule, expected_valid_row_count
        )

    def test_compare_length(self, polars_table):
        salary_rule_name = "check_compare_secteur_activite"
        salary_rule = F.compare_length(
            col_name="SECTEUR_ACTIVITE", operator=">", length=8
        )

        expected_valid_row_count = 80

        self.analyze_and_assert_rule(
            polars_table, salary_rule_name, salary_rule, expected_valid_row_count
        )

    def test_is_alphabetic(self, polars_table):
        salary_rule_name = "check_nom_is_alphabetic"
        salary_rule = F.is_alphabetic(col_name="NOM")

        expected_valid_row_count = 96

        self.analyze_and_assert_rule(
            polars_table, salary_rule_name, salary_rule, expected_valid_row_count
        )

    def test_is_negative(self, polars_table):
        salary_rule_name = "check_salaire_is_not_negative"
        salary_rule = F.is_negative(col_name="SALAIRE")

        expected_valid_row_count = 0

        self.analyze_and_assert_rule(
            polars_table, salary_rule_name, salary_rule, expected_valid_row_count
        )

    def test_is_positive(self, polars_table):
        salary_rule_name = "check_salaire_is_positive"
        salary_rule = F.is_positive(col_name="SALAIRE")

        expected_valid_row_count = 87

        self.analyze_and_assert_rule(
            polars_table, salary_rule_name, salary_rule, expected_valid_row_count
        )

    def test_not_condition(self, polars_table):
        salary_rule_name = "check_Prenom_not_not_null"
        salary_rule = ~F.is_not_null(col_name="PRENOM")

        expected_valid_row_count = 12

        self.analyze_and_assert_rule(
            polars_table, salary_rule_name, salary_rule, expected_valid_row_count
        )

    def analyze_and_assert_rule(
        self, polars_table, rule_name, rule, expected_valid_row_count
    ):
        computed_metrics = polars_table.analyze(rule_name, rule)
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

    def test_polars_table_from_dict(self):
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
        calista_table_from_dict = CalistaEngine("polars").load_from_dict(data_dict)

        assert_frame_equal(
            left=calista_table_from_dict._engine.dataset.lazy(),
            right=pl.LazyFrame(data_dict),
            check_row_order=False,
            check_column_order=False,
        )

    def test_analyze_rules(self, polars_table):
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

        computed_metrics = polars_table.analyze_rules(rules)

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

    def groupby_and_assert_rule(
        self,
        polars_table,
        keys,
        rule_name,
        rule,
        expected_valid_row_count,
        expected_dataset_row_count,
    ):
        computed_metrics = polars_table.group_by(keys).analyze(rule_name, rule)
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

    def test_compare_sum_to_value(self, polars_table):
        sum_by_rule_name = "check_sum_salary_by_sex_gt"
        sum_by_rule = F.sum_gt_value(col_name="SALAIRE", value=20000)

        expected_dataset_row_count = 3
        expected_valid_row_count = 3

        self.groupby_and_assert_rule(
            polars_table,
            "SEXE",
            sum_by_rule_name,
            sum_by_rule,
            expected_valid_row_count,
            expected_dataset_row_count,
        )

    def test_compare_count_to_value(self, polars_table):
        count_by_rule_name = "check_count_id_by_sex_gt"
        count_by_rule = F.count_gt_value(col_name="ID", value=5)

        expected_dataset_row_count = 3
        expected_valid_row_count = 2

        self.groupby_and_assert_rule(
            polars_table,
            "SEXE",
            count_by_rule_name,
            count_by_rule,
            expected_valid_row_count,
            expected_dataset_row_count,
        )

    def test_compare_mean_to_value(self, polars_table):
        mean_by_rule_name = "check_mean_salary_by_sex_gt"
        mean_by_rule = F.mean_gt_value(col_name="SALAIRE", value=10000)

        expected_dataset_row_count = 3
        expected_valid_row_count = 3

        self.groupby_and_assert_rule(
            polars_table,
            "SEXE",
            mean_by_rule_name,
            mean_by_rule,
            expected_valid_row_count,
            expected_dataset_row_count,
        )

    def test_compare_median_to_value(self, polars_table):
        median_by_rule_name = "check_median_salary_by_sex_gt"
        median_by_rule = F.median_gt_value(col_name="SALAIRE", value=70000)

        expected_dataset_row_count = 3
        expected_valid_row_count = 1

        self.groupby_and_assert_rule(
            polars_table,
            "SEXE",
            median_by_rule_name,
            median_by_rule,
            expected_valid_row_count,
            expected_dataset_row_count,
        )

    def test_compare_max_to_value(self, polars_table):
        max_by_rule_name = "check_max_salary_by_sex_ge"
        max_by_rule = F.max_ge_value(col_name="SALAIRE", value=50000)

        expected_dataset_row_count = 3
        expected_valid_row_count = 3

        self.groupby_and_assert_rule(
            polars_table,
            "SEXE",
            max_by_rule_name,
            max_by_rule,
            expected_valid_row_count,
            expected_dataset_row_count,
        )

    def test_compare_min_to_value(self, polars_table):
        min_by_rule_name = "check_min_salary_by_sex_lt"
        min_by_rule = F.min_lt_value(col_name="SALAIRE", value=50000)

        expected_dataset_row_count = 3
        expected_valid_row_count = 2

        self.groupby_and_assert_rule(
            polars_table,
            "SEXE",
            min_by_rule_name,
            min_by_rule,
            expected_valid_row_count,
            expected_dataset_row_count,
        )

    def test_multiple_agg(self, polars_table):
        rule_name = "check_sum_count_mean_salary_by_gender"
        rule = (
            F.sum_lt_value(col_name="SALAIRE", value=20000000)
            & F.count_le_value(col_name="SALAIRE", value=40)
            & F.mean_ge_value(col_name="SALAIRE", value=50000)
        )

        expected_dataset_row_count = 3
        expected_valid_row_count = 2

        self.groupby_and_assert_rule(
            polars_table,
            "SEXE",
            rule_name,
            rule,
            expected_valid_row_count,
            expected_dataset_row_count,
        )

    def test_agg_and_normal_cond_combination(self):
        with pytest.raises(Exception) as combination_exception:
            F.sum_gt_value(col_name="SALAIRE", value=20000) | F.is_iban("SALAIRE")

        assert "Cannot combine Condition with AggregateCondition" == str(
            combination_exception.value
        )

    def test_colname_not_in_table(self, polars_table):
        with pytest.raises(Exception) as combination_exception:
            polars_table.analyze("rule", F.is_iban("DATE"))

        assert (
            "Column 'DATE' not found in ['NOM', 'PRENOM', 'SEXE', 'DATE_ENTREE', 'CDI', 'IBAN', 'SECTEUR_ACTIVITE', 'ADRESSE', 'SITUATION_FAMILIALE', 'ADRESSE_IP_V4', 'ADRESSE_IP_V6', 'DATE_NAISSANCE', 'DATE_SORTIE', 'DATE_DERNIER_EA', 'DATE_DERNIERE_AUGMENTATION', 'CDD', 'EMAIL', 'TELEPHONE', 'SALAIRE', 'DEVISE', 'ID']"
            == str(combination_exception.value)
        )

    def test_col_left_not_in_table_compare_col_to_col(self, polars_table):
        with pytest.raises(Exception) as combination_exception:
            polars_table.analyze(
                "rule",
                F.compare_column_to_column(
                    col_left="DATE", operator="=", col_right="DATE_ENTREE"
                ),
            )

        assert (
            "Column 'DATE' not found in ['NOM', 'PRENOM', 'SEXE', 'DATE_ENTREE', 'CDI', 'IBAN', 'SECTEUR_ACTIVITE', 'ADRESSE', 'SITUATION_FAMILIALE', 'ADRESSE_IP_V4', 'ADRESSE_IP_V6', 'DATE_NAISSANCE', 'DATE_SORTIE', 'DATE_DERNIER_EA', 'DATE_DERNIERE_AUGMENTATION', 'CDD', 'EMAIL', 'TELEPHONE', 'SALAIRE', 'DEVISE', 'ID']"
            == str(combination_exception.value)
        )

    def test_col_right_not_in_table_compare_col_to_col(self, polars_table):
        with pytest.raises(Exception) as combination_exception:
            polars_table.analyze(
                "rule",
                F.compare_column_to_column(
                    col_left="DATE_ENTREE", operator="=", col_right="DATE"
                ),
            )

        assert (
            "Column 'DATE' not found in ['NOM', 'PRENOM', 'SEXE', 'DATE_ENTREE', 'CDI', 'IBAN', 'SECTEUR_ACTIVITE', 'ADRESSE', 'SITUATION_FAMILIALE', 'ADRESSE_IP_V4', 'ADRESSE_IP_V6', 'DATE_NAISSANCE', 'DATE_SORTIE', 'DATE_DERNIER_EA', 'DATE_DERNIERE_AUGMENTATION', 'CDD', 'EMAIL', 'TELEPHONE', 'SALAIRE', 'DEVISE', 'ID']"
            == str(combination_exception.value)
        )

    def test_col_not_in_table_groupby(self, polars_table):
        with pytest.raises(Exception) as combination_exception:
            polars_table.group_by("DATE")

        assert (
            "Column 'DATE' not found in ['NOM', 'PRENOM', 'SEXE', 'DATE_ENTREE', 'CDI', 'IBAN', 'SECTEUR_ACTIVITE', 'ADRESSE', 'SITUATION_FAMILIALE', 'ADRESSE_IP_V4', 'ADRESSE_IP_V6', 'DATE_NAISSANCE', 'DATE_SORTIE', 'DATE_DERNIER_EA', 'DATE_DERNIERE_AUGMENTATION', 'CDD', 'EMAIL', 'TELEPHONE', 'SALAIRE', 'DEVISE', 'ID']"
            == str(combination_exception.value)
        )
