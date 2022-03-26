from dagster import AssetGroup, asset
from dagster_gcp.gcs import gcs_pickle_asset_io_manager
from dagster_gcp import gcs_resource


@asset
def upstream_asset():
    return [1, 2, 3]


@asset
def downstream_asset(upstream_asset):
    return upstream_asset + [4]


asset_group = AssetGroup(
    [upstream_asset, downstream_asset],
    resource_defs={
        "io_manager": gcs_pickle_asset_io_manager.configured({
            "gcs_bucket": "dagster-test",
            "gcs_prefix": "asset_folder"
        }),
        "gcs": gcs_resource
    }
)

