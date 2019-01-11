import os
import json
import threading
import requests
import shutil
import time
import traceback

import zipfile
import shapefile

from confluent_kafka import Producer, KafkaError, KafkaException

def handler(context, event):

    b = event.body
    if not isinstance(b, dict):
        body = json.loads(b.decode('utf-8-sig'))
    else:
        body = b
    
    context.logger.info("Event received: " + str(body))

    try:

        # if we're not ready to handle this request yet, deny it
        if not FunctionState.done_loading:
            context.logger.warn_with('Function not ready, denying request !')
            raise NuclioResponseError(
                'The service is loading and is temporarily unavailable.',requests.codes.unavailable)
        
        # parse the event's payload
        b = Helpers.parse_body(context,body)

        archive_format = b['archive_format']
        archive_url = b['archive_url']
        source_id = b['source_id']
        parcel_id_field = b['parcel_id_field']
        normalized_parcel_properties = b['normalized_parcel_properties']
        version = b['version']

        target_topic = FunctionConfig.target_topic

        # download the requested archive
        tmp_dir = Helpers.create_temporary_dir(context,event)
        archive_target_path = os.path.join(tmp_dir,"archive.zip")
        Helpers.download_file(context,archive_url,archive_target_path)
        context.logger.info("Archive downloaded: " + archive_target_path)

        # extract the archive
        zipfile.ZipFile(archive_target_path,'r').extractall(tmp_dir)
        context.logger.info("Archive extracted !")

        # get the Kafka producer instance
        p = FunctionState.producer

        count = 0
        fail_count = 0

        # process shapefiles (only '*RECFE.shp' for now, cf. SIGPAC)
        files = [x for x in os.listdir(tmp_dir) if x.endswith('RECFE.shp')]
        start = time.time()
        for f in files:
            context.logger.info('Processing shapefile {0} ...'.format(os.path.basename(f)))
 
            reader = shapefile.Reader(os.path.join(tmp_dir,f))
            fields = reader.fields[1:]
            fields_name = [field[0] for field in fields]

            # CRS of the shapefile
            crs_code = FunctionConfig.source_crs_epsg_code
            # QUICK BUGFIX for shapefile of the year 2017 (has to be dynamic ...)
            if version == "2017":
                crs_code = FunctionConfig.source_crs_epsg_code_2017
            # legal CRS
            legal_crs_code = FunctionConfig.legal_es_crs_epsg_code
            for sr in reader.iterShapeRecords():
                parcel_properties = dict(zip(fields_name,sr.record))
                parcel_id = "{0}:{1}".format(source_id,int(parcel_properties[parcel_id_field]))

                properties = {
                    "parcel": parcel_properties,
                    "crs": {
                        "type": "EPSG",
                        "properties": {
                            "code": crs_code
                        }
                    },
                    "legal_crs": {
                        "type": "EPSG",
                        "properties": {
                            "code": legal_crs_code
                        }
                    },
                    "version": version
                }

                for prop in normalized_parcel_properties:
                    properties[prop] = parcel_properties.get(normalized_parcel_properties[prop])

                geometry = sr.shape.__geo_interface__

                # shape record in geojson format
                parcel = dict(_id=parcel_id, type="Feature", geometry=geometry, properties=properties)

                # send the geojson document to the target kafka topic
                value = json.dumps(parcel).encode("utf-8-sig")
                try:
                    p.produce(target_topic, value)
                except KafkaException as e:
                    # Quick fix: for now ignore MSG_SIZE_TOO_LARGE exception
                    if e.args[0].code() != KafkaError.MSG_SIZE_TOO_LARGE:
                        raise e
                    fail_count += 1

                if count % 1000 == 0:
                    p.flush()

                count += 1

        p.flush()

        context.logger.info('Shapefile {0} processed successfully ({1} geometries in {2}s).'
            .format(os.path.basename(f),count,round(time.time()-start)))
        context.logger.warn('Shapefile {0}: {1} geometries failed'.format(os.path.basename(f),fail_count))

    except NuclioResponseError as error:
        return error.as_response(context)

    except Exception as error:
        context.logger.warn_with('Unexpected error occurred, responding with internal server error',
            exc=str(error))
        message = 'Unexpected error occurred: {0}\n{1}'.format(error, traceback.format_exc())
        return NuclioResponseError(message).as_response(context)

    finally:
        shutil.rmtree(tmp_dir)

    return "Processing Completed ({0} geometries)".format(count)


class FunctionConfig(object):

    kafka_bootstrap_server = None

    target_topic = None

    accepted_source_id = 'lpis:es'

    source_crs_epsg_code = '4258'

    source_crs_epsg_code_2017 = '25830'

    # legal EPSG projection to calculate area, perimeter, etc ...
    legal_es_crs_epsg_code = '23030'


class FunctionState(object):

    producer = None

    done_loading = False


class Helpers(object):

    @staticmethod
    def load_configs():

        FunctionConfig.kafka_bootstrap_server = os.getenv('KAFKA_BOOTSTRAP_SERVER')

        FunctionConfig.target_topic = os.getenv('TARGET_TOPIC')

    @staticmethod
    def load_producer():

        p = Producer({
            'bootstrap.servers':  FunctionConfig.kafka_bootstrap_server})

        FunctionState.producer = p

    @staticmethod
    def parse_body(context, body):

        # parse archive's format
        if not ('format' in body and body['format'].lower() == 'zip'):
            context.logger.warn_with('Archive\'s \'format\' must be \'ZIP\' !')
            raise NuclioResponseError(
                'Archive\'s \'format\' must be \'ZIP\' !',requests.codes.bad)
        archive_format = body['format']

        # parse archive's URL
        if not 'url' in body:
            context.logger.warn_with('Missing \'url\' attribute !')
            raise NuclioResponseError(
                'Missing \'url\' attribute !',requests.codes.bad)
        archive_url = body['url']

        # parse archive's sourceID
        if not 'sourceID' in body:
            context.logger.warn_with('Missing \'sourceID\' attribute !')
            raise NuclioResponseError(
                'Missing \'sourceID\' attribute !',requests.codes.bad)

        # WORKAROUND: Check sourceID to only accept SIGPAC archives (SPAIN)
        # TODO: generalize ....
        if not body['sourceID'].startswith(FunctionConfig.accepted_source_id):
            context.logger.warn_with("sourceID: not a '{0}' data source !".format(FunctionConfig.accepted_source_id))
            raise NuclioResponseError(
                "sourceID: not a '{0}' data source !".format(FunctionConfig.accepted_source_id),requests.codes.bad)
        source_id = body['sourceID']

        # parse archive's parcelIdField
        if not 'parcelIdField' in body:
            context.logger.warn_with('Missing \'parcelIdField\' attribute !')
            raise NuclioResponseError(
                'Missing \'parcelIdField\' attribute !',requests.codes.bad)
        parcel_id_field = body['parcelIdField']

        # parse archive's normalizedProperties
        if not 'normalizedProperties' in body:
            context.logger.warn_with('Missing \'normalizedProperties\' attribute !')
            raise NuclioResponseError(
                'Missing \'normalizedProperties\' attribute !',requests.codes.bad)

        if not isinstance(body['normalizedProperties'],dict):
            context.logger.warn_with('\'normalizedProperties\' is not a dict !')
            raise NuclioResponseError(
                '\'normalizedProperties\' is not a dict !',requests.codes.bad)
        normalized_parcel_properties = body['normalizedProperties']

        # parse archive's version
        if not 'version' in body:
            context.logger.warn_with('Missing \'version\' attribute !')
            raise NuclioResponseError(
                'Missing \'version\' attribute !',requests.codes.bad)
        version = str(body['version'])

        b = {
            "archive_format": archive_format,
            "archive_url": archive_url,
            "source_id": source_id,
            "parcel_id_field": parcel_id_field,
            "normalized_parcel_properties": normalized_parcel_properties,
            "version": version
        }

        return b

    @staticmethod
    def create_temporary_dir(context, event):
        """
        Creates a uniquely-named temporary directory (based on the given event's id) and returns its path.
        """
        temp_dir = '/tmp/nuclio-event-{0}'.format(event.id)
        os.makedirs(temp_dir)

        context.logger.debug_with('Temporary directory created', path=temp_dir)

        return temp_dir

    @staticmethod
    def download_file(context, url, target_path):
        """
        Downloads the given remote URL to the specified path.
        """
        # make sure the target directory exists
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        try:
            with requests.get(url, stream=True) as response:
                response.raise_for_status()
                with open(target_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
        except Exception as error:
            if context is not None:
                context.logger.warn_with('Failed to download file',
                                         url=url,
                                         target_path=target_path,
                                         exc=str(error))
            raise NuclioResponseError('Failed to download file: {0}'.format(url),
                                      requests.codes.service_unavailable)
        if context is not None:
            context.logger.info_with('File downloaded successfully',
                                     size_bytes=os.stat(target_path).st_size,
                                     target_path=target_path)

    @staticmethod
    def on_import():

        Helpers.load_configs()
        Helpers.load_producer()
        FunctionState.done_loading = True


class NuclioResponseError(Exception):

    def __init__(self, description, status_code=requests.codes.internal_server_error):
        self._description = description
        self._status_code = status_code

    def as_response(self, context):
        return context.Response(body=self._description,
                                headers={},
                                content_type='text/plain',
                                status_code=self._status_code)


t = threading.Thread(target=Helpers.on_import)
t.start()
