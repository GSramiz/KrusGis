# utils/auth.py
import ee
import json
from google.oauth2 import service_account


def get_ee_service():
    credentials = ee.ServiceAccountCredentials(
        'gee-script@ee-romantik1994.iam.gserviceaccount.com',
        'service-account.json')
    ee.Initialize(credentials)
    return credentials
