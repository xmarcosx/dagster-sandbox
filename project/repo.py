from assets.edfi_asset import asset_job

from dagster import repository


@repository
def software_defined_assets():
    return [asset_job]
