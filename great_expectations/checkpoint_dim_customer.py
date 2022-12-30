#!/usr/bin/env python
# coding: utf-8

# ## Checkpoint dim_customer table

# In[1]:


from ruamel.yaml import YAML
import great_expectations as gx
from pprint import pprint

yaml = YAML()
context = gx.get_context()


# In[2]:


# Define checkpoint name and yaml config
my_checkpoint_name = "dim_customer_checkpoint" 

yaml_config = f"""
name: {my_checkpoint_name}
config_version: 1.0
class_name: SimpleCheckpoint
run_name_template: "%Y%m%d-%H%M%S-dim_customer"
validations:
  - batch_request:
      datasource_name: dim_customer_datasource
      data_connector_name: default_configured_data_connector_name
      data_asset_name: dim_customer
      data_connector_query:
        index: -1
    expectation_suite_name: suite_dim_customer
"""
print(yaml_config)


# In[3]:


# Get list of datasource and expectation suites that available
pprint(context.get_available_data_asset_names())
context.list_expectation_suite_names()


# In[4]:


# define and add new checkpoint
dim_customer_checkpoint = context.test_yaml_config(yaml_config=yaml_config)
print(dim_customer_checkpoint.get_config(mode="yaml"))
context.add_checkpoint(**yaml.load(yaml_config))


# In[5]:


# run checkpoint
context.run_checkpoint(checkpoint_name=my_checkpoint_name)
context.open_data_docs()

