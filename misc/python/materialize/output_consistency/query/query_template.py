# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from collections.abc import Callable

from materialize.output_consistency.execution.evaluation_strategy import (
    EvaluationStrategy,
)
from materialize.output_consistency.execution.query_output_mode import (
    QueryOutputMode,
    query_output_mode_to_sql,
)
from materialize.output_consistency.execution.sql_dialect_adjuster import (
    SqlDialectAdjuster,
)
from materialize.output_consistency.execution.value_storage_layout import (
    ROW_INDEX_COL_NAME,
    ValueStorageLayout,
)
from materialize.output_consistency.expression.expression import Expression
from materialize.output_consistency.expression.expression_characteristics import (
    ExpressionCharacteristics,
)
from materialize.output_consistency.query.additional_data_source import (
    AdditionalDataSource,
)
from materialize.output_consistency.query.data_source import (
    DataSource,
)
from materialize.output_consistency.query.query_format import QueryOutputFormat
from materialize.output_consistency.selection.selection import (
    DataRowSelection,
    QueryColumnByIndexSelection,
)


class QueryTemplate:
    """Query template as base for creating SQL for different evaluation strategies"""

    def __init__(
        self,
        expect_error: bool,
        select_expressions: list[Expression],
        where_expression: Expression | None,
        storage_layout: ValueStorageLayout,
        data_source: DataSource,
        contains_aggregations: bool,
        row_selection: DataRowSelection,
        offset: int | None = None,
        limit: int | None = None,
        additional_data_sources: list[AdditionalDataSource] = [],
        custom_order_expressions: list[Expression] | None = None,
    ) -> None:
        assert storage_layout != ValueStorageLayout.ANY
        self.expect_error = expect_error
        self.select_expressions: list[Expression] = select_expressions
        self.where_expression = where_expression
        self.storage_layout = storage_layout
        self.data_source = data_source
        self.additional_data_sources = additional_data_sources
        self.contains_aggregations = contains_aggregations
        self.row_selection = row_selection
        self.offset = offset
        self.limit = limit
        self.custom_order_expressions = custom_order_expressions
        self.disable_error_message_validation = not self.__can_compare_error_messages()

    def get_all_data_sources(self) -> list[DataSource]:
        all_data_sources = [self.data_source]
        all_data_sources.extend(self.additional_data_sources)
        return all_data_sources

    def get_all_expressions(
        self,
        include_select_expressions: bool = True,
        include_join_constraints: bool = False,
    ) -> list[Expression]:
        all_expressions = []

        if include_select_expressions:
            all_expressions.extend(self.select_expressions)

        if self.where_expression is not None:
            all_expressions.append(self.where_expression)

        if self.custom_order_expressions is not None:
            all_expressions.extend(self.custom_order_expressions)

        if include_join_constraints:
            for additional_data_source in self.additional_data_sources:
                all_expressions.append(additional_data_source.join_constraint)

        return all_expressions

    def to_sql(
        self,
        strategy: EvaluationStrategy,
        output_format: QueryOutputFormat,
        query_column_selection: QueryColumnByIndexSelection,
        query_output_mode: QueryOutputMode,
        override_db_object_name: str | None = None,
    ) -> str:
        space_separator = self._get_space_separator(output_format)

        column_sql = self._create_column_sql(
            query_column_selection, space_separator, strategy.sql_adjuster
        )
        from_clause = self._create_from_clause(
            strategy, override_db_object_name, strategy.sql_adjuster, space_separator
        )
        join_clauses = self._create_join_clauses(
            strategy, override_db_object_name, strategy.sql_adjuster, space_separator
        )
        where_clause = self._create_where_clause(strategy.sql_adjuster)
        order_by_clause = self._create_order_by_clause(strategy.sql_adjuster)
        limit_clause = self._create_limit_clause()
        offset_clause = self._create_offset_clause()

        explain_mode = query_output_mode_to_sql(query_output_mode)

        sql = f"""
{explain_mode} SELECT{space_separator}{column_sql}
{from_clause}
{join_clauses}
{where_clause}
{order_by_clause}
{limit_clause}
{offset_clause};
""".strip()

        return self._post_format_sql(sql, output_format)

    def _get_space_separator(self, output_format: QueryOutputFormat) -> str:
        return "\n  " if output_format == QueryOutputFormat.MULTI_LINE else " "

    def _create_column_sql(
        self,
        query_column_selection: QueryColumnByIndexSelection,
        space_separator: str,
        sql_adjuster: SqlDialectAdjuster,
    ) -> str:
        expressions_as_sql = []
        for index, expression in enumerate(self.select_expressions):
            if query_column_selection.is_included(index):
                expressions_as_sql.append(expression.to_sql(sql_adjuster, True))

        return f",{space_separator}".join(expressions_as_sql)

    def _create_from_clause(
        self,
        strategy: EvaluationStrategy,
        override_db_object_name: str | None,
        sql_adjuster: SqlDialectAdjuster,
        space_separator: str,
    ) -> str:
        db_object_name = self.data_source.get_db_object_name(
            strategy,
            self.storage_layout,
            override_db_object_name=override_db_object_name,
        )
        return f"FROM{space_separator}{db_object_name}"

    def _create_join_clauses(
        self,
        strategy: EvaluationStrategy,
        override_db_object_name: str | None,
        sql_adjuster: SqlDialectAdjuster,
        space_separator: str,
    ) -> str:
        if len(self.additional_data_sources) == 0:
            # no JOIN necessary
            return ""

        join_clauses = ""

        for additional_data_source in self.additional_data_sources:
            join_clauses = (
                f"{join_clauses}"
                f"\n{self._create_join_clause(strategy, additional_data_source, override_db_object_name, sql_adjuster, space_separator)}"
            )

        return join_clauses

    def _create_join_clause(
        self,
        strategy: EvaluationStrategy,
        additional_data_source_to_join: AdditionalDataSource,
        override_db_object_name: str | None,
        sql_adjuster: SqlDialectAdjuster,
        space_separator: str,
    ) -> str:
        db_object_name_to_join = additional_data_source_to_join.get_db_object_name(
            strategy,
            self.storage_layout,
            override_db_object_name=override_db_object_name,
        )

        join_operator_sql = additional_data_source_to_join.join_operator.to_sql()

        return (
            f"{join_operator_sql} {db_object_name_to_join} {additional_data_source_to_join.alias()}"
            f"{space_separator}ON {additional_data_source_to_join.join_constraint.to_sql(sql_adjuster, True)}"
        )

    def _create_where_clause(self, sql_adjuster: SqlDialectAdjuster) -> str:
        where_conditions = []

        row_filter_clause = self._create_row_filter_clause()
        if row_filter_clause:
            where_conditions.append(row_filter_clause)

        if self.where_expression:
            where_conditions.append(self.where_expression.to_sql(sql_adjuster, True))

        if len(where_conditions) == 0:
            return ""

        # It is important that the condition parts are in parentheses so that they are connected with AND.
        # Otherwise, a generated condition containing OR at the top level may lift the row filter clause.
        all_conditions_sql = " AND ".join(
            [f"({condition})" for condition in where_conditions]
        )
        return f"WHERE {all_conditions_sql}"

    def _create_row_filter_clause(self) -> str | None:
        """Create s SQL clause to only include rows of certain indices"""
        if self.row_selection.keys is None:
            return None

        if len(self.row_selection.keys) == 0:
            row_index_string = "-1"
        else:
            row_index_string = ", ".join(
                str(index) for index in self.row_selection.keys
            )
        return f"{ROW_INDEX_COL_NAME} IN ({row_index_string})"

    def _create_order_by_clause(self, sql_adjuster: SqlDialectAdjuster) -> str:
        if self.custom_order_expressions is not None:
            order_by_specs_str = ", ".join(
                [
                    f"{expr.to_sql(sql_adjuster, True)} ASC"
                    for expr in self.custom_order_expressions
                ]
            )
            return f"ORDER BY {order_by_specs_str}"

        if (
            self.storage_layout == ValueStorageLayout.VERTICAL
            and not self.contains_aggregations
        ):
            return f"ORDER BY {ROW_INDEX_COL_NAME} ASC"

        return ""

    def _create_offset_clause(self) -> str:
        if self.offset is not None:
            return f"OFFSET {self.offset}"

        return ""

    def _create_limit_clause(self) -> str:
        if self.limit is not None:
            return f"LIMIT {self.limit}"

        return ""

    def _post_format_sql(self, sql: str, output_format: QueryOutputFormat) -> str:
        # apply this replacement twice
        sql = sql.replace("\n\n", "\n").replace("\n\n", "\n")
        sql = sql.replace("\n;", ";")

        if output_format == QueryOutputFormat.SINGLE_LINE:
            sql = sql.replace("\n", " ")

        return sql

    def collect_involved_vertical_table_indices(self) -> set[int] | None:
        if self.storage_layout == ValueStorageLayout.HORIZONTAL:
            return None

        assert self.storage_layout == ValueStorageLayout.VERTICAL
        table_indices = set()

        all_expressions = []
        all_expressions.extend(self.select_expressions)
        all_expressions.append(self.where_expression)
        all_expressions.extend(self.custom_order_expressions or [])

        for expression in all_expressions:
            table_indices.update(expression.collect_vertical_table_indices())

        return table_indices

    def column_count(self) -> int:
        return len(self.select_expressions)

    def __can_compare_error_messages(self) -> bool:
        if self.storage_layout == ValueStorageLayout.HORIZONTAL:
            return True

        for expression in self.select_expressions:
            if expression.contains_leaf_not_directly_consumed_by_aggregation():
                # The query operates on multiple rows and contains at least one non-aggregate function directly
                # operating on the value. Since the row processing order is not fixed, different evaluation
                # strategies may yield different error messages (depending on the first invalid value they
                # encounter). Therefore, error messages shall not be compared in case of a query failure.
                return False

        return True

    def matches_any_select_expression(
        self, predicate: Callable[[Expression], bool], check_recursively: bool
    ) -> bool:
        for expression in self.select_expressions:
            if expression.matches(predicate, check_recursively):
                return True

        return False

    def matches_any_expression(
        self, predicate: Callable[[Expression], bool], check_recursively: bool
    ) -> bool:
        return self.matches_any_select_expression(predicate, check_recursively) or (
            self.where_expression is not None
            and self.where_expression.matches(predicate, check_recursively)
        )

    def matches_specific_select_or_filter_expression(
        self,
        select_column_index: int,
        predicate: Callable[[Expression], bool],
        check_recursively: bool,
    ) -> bool:
        assert 0 <= select_column_index <= self.column_count()
        return self.select_expressions[select_column_index].matches(
            predicate, check_recursively
        ) or (
            self.where_expression is not None
            and self.where_expression.matches(predicate, check_recursively)
        )

    def get_involved_characteristics(
        self,
        query_column_selection: QueryColumnByIndexSelection,
    ) -> set[ExpressionCharacteristics]:
        all_involved_characteristics: set[ExpressionCharacteristics] = set()

        for index, expression in enumerate(self.select_expressions):
            if not query_column_selection.is_included(index):
                continue

            characteristics = expression.recursively_collect_involved_characteristics(
                self.row_selection
            )
            all_involved_characteristics = all_involved_characteristics.union(
                characteristics
            )

        return all_involved_characteristics
