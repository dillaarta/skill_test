# ## Checkpoint fact_order_accumulating table

from ruamel.yaml import YAML
import great_expectations as gx
from pprint import pprint

def get_checkpoint():
  yaml = YAML()
  context = gx.get_context()


  # Define checkpoint name and yaml config
  my_checkpoint_name = "fact_order_accumulating_checkpoint" 

  yaml_config = f"""
  name: {my_checkpoint_name}
  config_version: 1.0
  class_name: SimpleCheckpoint
  run_name_template: "%Y%m%d-%H%M%S-fact_order_accumulating"
  validations:
    - batch_request:
        datasource_name: fact_order_accumulating_datasource
        data_connector_name: default_configured_data_connector_name
        data_asset_name: fact_order_accumulating
        data_connector_query:
          index: -1
      expectation_suite_name: suite_fact_order_accumulating
  """
  print(yaml_config)

  # define and add new checkpoint
  fact_order_accumulating_checkpoint = context.test_yaml_config(yaml_config=yaml_config)
  print(fact_order_accumulating_checkpoint.get_config(mode="yaml"))
  context.add_checkpoint(**yaml.load(yaml_config))

  # run checkpoint
  check = context.run_checkpoint(checkpoint_name=my_checkpoint_name)
  context.open_data_docs()
  return check

