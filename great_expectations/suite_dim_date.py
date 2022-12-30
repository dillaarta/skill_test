#!/usr/bin/env python
# coding: utf-8

# ## Expectation Suite dim_date

# In[1]:


import datetime

import pandas as pd

import great_expectations as gx
import great_expectations.jupyter_ux
from great_expectations.core.batch import BatchRequest
from great_expectations.core import ExpectationConfiguration
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.exceptions import DataContextError

context = gx.data_context.DataContext()

# define batch request
batch_request = {'datasource_name': 'dim_date_datasource', 'data_connector_name': 'default_configured_data_connector_name', 'data_asset_name': 'dim_date'}

expectation_suite_name = "suite_dim_date"
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


# In[2]:


# Table expectation
column_list= ["id",
    "date",
    "month",
    "quarter_of_year", "year",
    "is_weekend"]
validator.expect_table_columns_to_match_ordered_list(column_list)


# In[3]:


for column in column_list:
    validator.expect_column_to_exist(column=column)


# In[4]:


# Column expectation
for columns in column_list:
    validator.expect_column_values_to_not_be_null(columns)


# In[5]:


col_list= ["id",
    "date",
    "month"]
for cols in col_list:
    validator.expect_column_values_to_be_unique(cols)


# In[6]:


types = {"id": "INTEGER",
    "date" : "DATE",
    "month": "INTEGER",
    "quarter_of_year": "INTEGER",
    "year": "INTEGER",
    "is_weekend": "BOOLEAN"}

for column, type_ in types.items():
    validator.expect_column_values_to_be_of_type(column=column, type_=type_)


# In[7]:


# Validate expectation suite
print(validator.get_expectation_suite(discard_failed_expectations=False))
validator.save_expectation_suite(discard_failed_expectations=False)

checkpoint_config = {
    "class_name": "SimpleCheckpoint",
    "validations": [
        {
            "batch_request": batch_request,
            "expectation_suite_name": "suite_dim_date"
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

