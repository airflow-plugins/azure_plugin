from airflow.plugins_manager import AirflowPlugin
from AzureDWPlugin.operators.s3_to_azuredw_operator import S3ToAzureDWOperator
from AzureDWPlugin.hooks.azure_dw_hook import AzureDWHook


class S3ToAzureDataWarehousePlugin(AirflowPlugin):
    name = "S3ToAzureDataWarehousePlugin"
    operators = [S3ToAzureDWOperator]
    # Leave in for explicitness
    hooks = [AzureDWHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
