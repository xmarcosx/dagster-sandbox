import os

from dagster import AssetGroup, asset
from dagster_gcp.gcs import gcs_pickle_asset_io_manager
from dagster_gcp import gcs_resource

from resources.edfi_api_resource import edfi_api_resource_client


@asset(required_resource_keys={"edfi_api_client"})
def edfi_schools(context):
    for yielded_response in context.resources.edfi_api_client.get_data(
        "/ed-fi/schools", 2022, None, None
    ):

        for record in yielded_response:
            context.log.debug(record)


@asset
def upstream_asset():
    return [1, 2, 3]


@asset
def downstream_asset(upstream_asset):
    return upstream_asset + [4]


asset_group = AssetGroup(
    [edfi_schools, upstream_asset, downstream_asset],
    resource_defs={
        "io_manager": gcs_pickle_asset_io_manager.configured(
            {"gcs_bucket": "dagster-test", "gcs_prefix": "asset_folder"}
        ),
        "gcs": gcs_resource,
        "edfi_api_client": edfi_api_resource_client.configured(
            {
                "base_url": os.getenv("EDFI_BASE_URL"),
                "api_key": os.getenv("EDFI_API_KEY"),
                "api_secret": os.getenv("EDFI_API_SECRET"),
                "api_page_limit": 100,
                "api_mode": "SharedInstance",
            }
        ),
    },
)


asset_job = asset_group.build_job(name="my_asset_job")
