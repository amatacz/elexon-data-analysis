import functions_framework

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
    # GCloudIntegratorObject.get_secret("elexon-project-service-account-secret")

    # Get yesterday's date
    yesterday_date = DataConfiguratorObject.timeframe_window()

    availability_data = DataExtractorObject.get_availability_data(yesterday_date)

    # for file in get_availability_data upload file to bucket
    for file in availability_data:
        availability_data_file = DataExtractorObject.download_files_from_availability_data(filename=file)
        GCloudIntegratorObject.upload_data_to_cloud_from_string("elexon-project-raw-data-bucket", availability_data_file, file)

    kafka.main(f"{yesterday_date}_filenames", availability_data)

    return "DONE"
