# ##  Expectation Suite fact_order_acumulating

import datetime

import pandas as pd

import great_expectations as gx
import great_expectations.jupyter_ux
from great_expectations.core.batch import BatchRequest
from great_expectations.core import ExpectationConfiguration
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.exceptions import DataContextError

context = gx.data_context.DataContext()

def get_data_suite():
    # define batch request
    batch_request = {'datasource_name': 'fact_order_accumulating_datasource', 'data_connector_name': 'default_configured_data_connector_name', 'data_asset_name': 'fact_order_accumulating'}

    expectation_suite_name = "suite_fact_order_accumulating"
    try:
        suite = context.get_expectation_suite(expectation_suite_name=expectation_suite_name)
        print(f'Loaded ExpectationSuite "{suite.expectation_suite_name}" containing {len(suite.expectations)} expectations.')
    except DataContextError:
        suite = context.create_expectation_suite(expectation_suite_name=expectation_suite_name)
        print(f'Created ExpectationSuite "{suite.expectation_suite_name}".')


    validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite_name=expectation_suite_name
    )
    column_names = [f'"{column_name}"' for column_name in validator.columns()]
    print(f"Columns: {', '.join(column_names)}.")
    validator.head(n_rows=5, fetch_all=False)

    # Table expectation
    column_list= ["orders_date_id",
        "invoices_date_id",
        "payments_date_id",
        "customer_id", "order_number",
        "invoice_number",
        "payment_number",
        "total_order_quantity",
        "total_order_usd_amount",
        "order_to_invoice_lag_days",
        "invoice_to_payment_lag_days"]
    validator.expect_table_columns_to_match_ordered_list(column_list)

    for column in column_list:
        validator.expect_column_to_exist(column=column)

    # Column expectation
    for columns in column_list:
        validator.expect_column_values_to_not_be_null(columns)

    for columns in column_list:
        validator.expect_column_values_to_be_unique(columns)

    types = {"orders_date_id": "INTEGER",
        "invoices_date_id": "INTEGER",
        "payments_date_id": "INTEGER",
        "customer_id": "INTEGER",
        "order_number" : "VARCHAR",
        "invoice_number" : "TEXT",
        "payment_number" : "TEXT",
        "total_order_quantity": "BIGINT",
        "total_order_usd_amount": "NUMERIC",
        "order_to_invoice_lag_days": "INTEGER",
        "invoice_to_payment_lag_days": "INTEGER",}

    for column, type_ in types.items():
        validator.expect_column_values_to_be_of_type(column=column, type_=type_)

    # Validate expectation suite
    print(validator.get_expectation_suite(discard_failed_expectations=False))
    validator.save_expectation_suite(discard_failed_expectations=False)

    checkpoint_config = {
        "class_name": "SimpleCheckpoint",
        "validations": [
            {
                "batch_request": batch_request,
                "expectation_suite_name": "suite_fact_order_accumulating"
            }
        ]
    }
    checkpoint = SimpleCheckpoint(
        f"{validator.active_batch_definition.data_asset_name}_{expectation_suite_name}",
        context,
        **checkpoint_config
    )
    checkpoint_result = checkpoint.run()

    context.build_data_docs()

    validation_result_identifier = checkpoint_result.list_validation_result_identifiers()[0]
    context.open_data_docs(resource_identifier=validation_result_identifier)
    return validation_result_identifier
