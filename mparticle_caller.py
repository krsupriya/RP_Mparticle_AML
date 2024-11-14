import mparticle
import time
import os
from os import getenv
# from mparticle.rest import ApiException
from mparticle import rest
import ssl
import certifi
import urllib3
import ssl
import certifi
import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager



timeout = float(os.getenv('SLEEP_BETWEEN_REQUESTS_SECONDS','0.02'))
print(timeout)


# def call_mparticle(api_instance, batch, timeout=timeout):
#     print(timeout)
#     try:
#         print("start call")
#         time.sleep(timeout)
#         if type(batch) == list:
#             print("entering if ")
#             api_instance.bulk_upload_events(batch)
#             print("exit if")
#             print("end call")
#         else:
#             api_instance.upload_events(batch)
#     except mparticle.rest.ApiException as error_response:
#         print("start mparticle exception test")
#         handle_api_exception(api_instance, batch, error_response, timeout)
#         print("end mparticle exception test")


class SSLAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        context = ssl.create_default_context(cafile=certifi.where())
        kwargs['ssl_context'] = context
        return super(SSLAdapter, self).init_poolmanager(*args, **kwargs)

# Apply the SSL adapter to the requests session
session = requests.Session()
session.mount('https://', SSLAdapter())
def call_mparticle(api_instance, batch, timeout=0.02):
    print("timeout")
    try:
        print("start call")
        time.sleep(timeout)
        print("trying to enter if but cant")
        if type(batch) == list:
            print("entering if ")
            api_instance.bulk_upload_events(batch)
            print("exit if")
            print("end call")
        else:
            # Set SSL context
            print("entering else 1 ")
            api_instance.api_client.rest_client.pool_manager = session
            print("making api calls ")
            print(type(api_instance))
            print(type(batch))
            api_instance.upload_events(batch)
    except mparticle.rest.ApiException as error_response:
        print("start mparticle exception test")
        handle_api_exception(api_instance, batch, error_response, timeout)
        print("end mparticle exception test")
def handle_api_exception(api_instance, batch, error, timeout):
    """Handle error responses from mParticle"""
    if has_to_be_retried(error.status) and timeout < 60:
        new_timeout = timeout * 2
        print(f"RETRYING AFTER {new_timeout}")
        print(error)
        call_mparticle(api_instance, batch, new_timeout)
    else:
        print(f"Exception while calling mParticle: {error}\n")
        raise error


def has_to_be_retried(response_status):
    """Check if request must be retried"""
    return response_status == 408 or response_status == 429 or 500 <= response_status <= 599
