import os
import json
import threading
import requests
import traceback

from bs4 import BeautifulSoup
from confluent_kafka import Producer, KafkaError

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
        year, province, municipality = Helpers.parse_body(context,body)
        url, code = [], ""
        baseurl = FunctionConfig.lpis_sigpac_baseurl

        context.logger.info("LPIS SIGPAC baseURL: {0}".format(baseurl))

        def scrap_hyperlinks(url,start_with):
            r = requests.get(url)
            bs = BeautifulSoup(r.content,'html.parser')
            links = bs.findAll('a', href=True)
            result = [a for a in links if a.text.startswith(start_with)]
            return result

        url.append(baseurl)
        url.append("{0}_ETRS89".format(year))

        if municipality is not None:
            url.append('Parcelario_SIGPAC_CyL_Municipios')
            code = "{0}{1}".format(province,municipality)
        else:
            url.append('Parcelario_SIGPAC_CyL_Provincias')
            code = province

        url = '/'.join(x for x in url)
        hls = scrap_hyperlinks(url,"{0}_".format(province))

        if not len(hls) == 1:
            context.logger.warn_with("Failed to scrape province code '{0}', result={1}".format(province,hls))
            raise NuclioResponseError(
                "Failed to scrape province code '{0}', result={1}".format(province,hls),requests.codes.bad)

        context.logger.info("Province code '{0}' successfully scraped, result={1}".format(province,hls))

        url = '/'.join([url,hls[0].attrs['href']]).strip()

        if municipality is not None:
            hls = scrap_hyperlinks(url,"{0}_".format(code))
            if not len(hls) == 1:
                context.logger.warn_with("Failed to scrape province+municipality code '{0}', result={1}".format(code,hls))
                raise NuclioResponseError(
                    "Failed to scrape province+municipality code '{0}', result={1}".format(code,hls),requests.codes.bad)
            context.logger.info("Province+Municipality code '{0}' successfully scraped, result={1}".format(code,hls))
            url = '/'.join([url,hls[0].attrs['href']])

        if not url.lower().endswith('.zip'):
            context.logger.warn_with("The targeted file is not in a ZIP archive, url={0}".format(url))
            raise NuclioResponseError(
                "The targeted file is not in a ZIP archive, url={0}".format(url),requests.codes.bad)

        context.logger.info("URL: {0}".format(url))
        
        source_id = FunctionConfig.source_id
        parcel_id_field = FunctionConfig.parcel_id_field
        normalized_parcel_properties = FunctionConfig.normalized_parcel_properties

        download_cmd = {
            "format": "zip", 
            "url": url, 
            "sourceID": source_id, 
            "parcelIdField": parcel_id_field,
            "normalizedProperties": normalized_parcel_properties,
            "version": year
        }

        p = FunctionState.producer
        target_topic = FunctionConfig.target_topic

        value = json.dumps(download_cmd).encode("utf-8-sig")
        p.produce(target_topic, value)
        p.flush()

        context.logger.info('Download command successfully submitted: {0}'.format(value))

    except NuclioResponseError as error:
        return error.as_response(context)

    except Exception as error:
        context.logger.warn_with('Unexpected error occurred, responding with internal server error',
            exc=str(error))
        message = 'Unexpected error occurred: {0}\n{1}'.format(error, traceback.format_exc())
        return NuclioResponseError(message).as_response(context)

    return context.Response(body=json.dumps({"message": "Archive '{0}' successfully submitted for ingestion".format(url)}),
                            headers={},
                            content_type='application/json',
                            status_code=requests.codes.ok)


class FunctionConfig(object):

    kafka_bootstrap_server = None

    target_topic = None

    source_id = "lpis:es:castillayleon"

    parcel_id_field = "DN_OID"

    normalized_parcel_properties = dict([
        ('area','SUPERFICIE'),
        ('perimeter','PERIMETRO')])

    lpis_sigpac_baseurl = None


class FunctionState(object):

    producer = None

    done_loading = False


class Helpers(object):

    @staticmethod
    def load_configs():

        FunctionConfig.kafka_bootstrap_server = os.getenv('KAFKA_BOOTSTRAP_SERVER')

        FunctionConfig.target_topic = os.getenv('TARGET_TOPIC')

        FunctionConfig.lpis_sigpac_baseurl = os.getenv('LPIS_SIGPAC_BASEURL','http://ftp.itacyl.es/cartografia/05_SIGPAC')

    @staticmethod
    def load_producer():

        p = Producer({
            'bootstrap.servers':  FunctionConfig.kafka_bootstrap_server})

        FunctionState.producer = p

    @staticmethod
    def parse_body(context, body):

        # parse year
        if not 'year' in body:
            context.logger.warn_with('Missing \'year\' attribute !')
            raise NuclioResponseError(
                'Missing \'year\' attribute !',requests.codes.bad)
        if not int(body['year']):
            context.logger.warn_with('\'year\' attribute must be an integer !')
            raise NuclioResponseError(
                '\'year\' attribute must be an integer !',requests.codes.bad)
        year = body['year']

        # parse province
        if not 'province' in body:
            context.logger.warn_with('Missing \'province\' attribute !')
            raise NuclioResponseError(
                'Missing \'province\' attribute !',requests.codes.bad)
        if not int(body['province']):
            context.logger.warn_with('\'province\' attribute must be an integer !')
            raise NuclioResponseError(
                '\'province\' attribute must be an integer !',requests.codes.bad)
        province = body['province']

        municipality = None
        if 'municipality' in body:
            if not int(body['municipality']):
                context.logger.warn_with('\'municipality\' attribute must be an integer !')
                raise NuclioResponseError(
                '\'municipality\' attribute must be an integer !',requests.codes.bad)
            else:
                municipality = body['municipality']

        return year, province, municipality

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
        return context.Response(body=json.dumps({"message": self._description}),
                                headers={},
                                content_type='application/json',
                                status_code=self._status_code)


t = threading.Thread(target=Helpers.on_import)
t.start()
