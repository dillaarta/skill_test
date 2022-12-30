#!/usr/bin/env python
# coding: utf-8

# ## Datasource dim_customer

# In[7]:


# Define name of datasource
import great_expectations as gx
from great_expectations.cli.datasource import sanitize_yaml_and_save_datasource, check_if_datasource_name_exists
context = gx.get_context()
datasource_name = "dim_customer_datasource"


# In[8]:


# Define connection and table name
host = "127.0.0.1"
port = "5432"
username = "postgres"
password = "5432"
database = "postgres"
schema_name = "public"

# A table that will add initially as a Data Asset
table_name = "dim_customer"


# In[9]:


# yaml config
example_yaml = f"""
name: {datasource_name}
class_name: Datasource
execution_engine:
  class_name: SqlAlchemyExecutionEngine
  credentials:
    host: {host}
    port: '{port}'
    username: {username}
    password: {password}
    database: {database}
    drivername: postgresql
data_connectors:
  default_runtime_data_connector_name:
    class_name: RuntimeDataConnector
    batch_identifiers:
      - default_identifier_namefact
  default_inferred_data_connector_name:
    class_name: InferredAssetSqlDataConnector
    include_schema_name: True
    introspection_directives:
      schema_name: {schema_name}
  default_configured_data_connector_name:
    class_name: ConfiguredAssetSqlDataConnector
    assets:
      {table_name}:
        class_name: Asset
        schema_name: {schema_name}
"""
print(example_yaml)


# In[10]:


context.test_yaml_config(yaml_config=example_yaml)


# In[11]:


sanitize_yaml_and_save_datasource(context, example_yaml, overwrite_existing=False)
context.list_datasources()

