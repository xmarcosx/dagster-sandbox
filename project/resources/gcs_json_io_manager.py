from dagster import io_manager

class GCSJsonIOManager(PickledObjectGCSIOManager):
    def __init__(self, bucket, client=None, prefix="dagster"):
        super().__init__(bucket, client, prefix)

    def _get_path(self, context, **kwargs):
        file_key = kwargs.get("file_key")
        if file_key:
            return "/".join([self.prefix, file_key])
        else:
            parts = context.get_output_identifier()
            run_id = parts[0]
            output_parts = parts[1:]
            return "/".join([self.prefix, "storage", run_id, "files", *output_parts])

    def load_input(self, context):
        key = self._get_path(context.upstream_output)
        context.log.debug(f"Loading GCS object from: {self._uri_for_key(key)}")

        bytes_obj = self.bucket_obj.blob(key).download_as_bytes()
        obj = json.loads(bytes_obj)

        return obj

    def handle_output(self, context, obj):
        data, file_key = obj
        key = self._get_path(context, file_key=file_key)
        context.log.debug(f"Writing GCS object at: {self._uri_for_key(key)}")

        if self._has_object(key):
            context.log.warning(f"Removing existing GCS key: {key}")
            self._rm_object(key)

        json_obj = json.dumps(data)

        backoff(
            self.bucket_obj.blob(key).upload_from_string,
            args=[json_obj],
            retry_on=(TooManyRequests, Forbidden),
        )

@io_manager(
    config_schema={"base_path": Field(str, is_required=False), "duckdb_path": str},
    required_resource_keys={"pyspark"},
)
def gcs_json_io_manager(init_context):
    return GCSJsonIOManager(
        base_path=init_context.resource_config.get("base_path", get_system_temp_directory())
    )
