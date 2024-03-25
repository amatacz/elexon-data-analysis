import functions_framework
import json

from shared.gcloud_integrator import GCloudIntegrator
from models.data_extractor import DataExtractor
from shared.utils import DataConfigurator
from models.kafka import kafka


@functions_framework.http
def get_elexon_data_and_send_it_to_kafka(request, context=None):

    # Create DataExtractor Object
    DataExtractorObject = DataExtractor()

    # Create GCloudIntegrator Object
    GCloudIntegratorObject = GCloudIntegrator("elexon-project")

    # Create timeframe object
    DataConfiguratorObject = DataConfigurator()

    # Get project secret
    GCloudIntegratorObject.get_secret("elexon-project-service-account-secret")

    # Get yesterday's date
    yesterday_date = DataConfiguratorObject.timeframe_window()

    availability_data = DataExtractorObject.get_availability_data(yesterday_date)

    if availability_data:

        availability_data_bytes = json.dumps(list(availability_data.keys()), indent=2).encode('utf-8')

        # Send filenames to kafka
        kafka.main(f"{yesterday_date}_filenames", availability_data_bytes)

        # for file in get_availability_data upload file to bucket
        for file in availability_data:
            availability_data_file = DataExtractorObject.download_files_from_availability_data(filename=file)
            GCloudIntegratorObject.upload_data_to_cloud_from_string("elexon-project-raw-data-bucket", availability_data_file, file)

        return f"Data from {yesterday_date} fetched."
    else:
        return f"No data available from date: {yesterday_date}"
