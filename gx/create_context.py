import great_expectations as gx

context = gx.get_context(mode="file", project_root_dir="./")
# SETUP FOR THE EXAMPLE:
data_source = context.data_sources.add_spark(name="my_data_source")
data_asset = data_source.add_dataframe_asset(name="my_dataframe_data_asset")

# Retrieve the Data Asset
data_source_name = "my_data_source"
data_asset_name = "my_dataframe_data_asset"
data_asset = context.data_sources.get(data_source_name).get_asset(data_asset_name)

# Define the Batch Definition name
batch_definition_name = "my_batch_definition"

# Add a Batch Definition to the Data Asset
batch_definition = data_asset.add_batch_definition_whole_dataframe(
    batch_definition_name
)
