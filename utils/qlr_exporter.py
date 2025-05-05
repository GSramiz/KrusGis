# utils/qlr_exporter.py
import xml.etree.ElementTree as ET
import os
import tempfile
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

def generate_qlr_file(xyz_url: str, filename: str) -> str:
    qlr = ET.Element('qgis', version="3.16")
    layer = ET.SubElement(qlr, 'layer-tree-layer', name=filename.replace('.qlr', ''), providerKey="wms")

    layer_elem = ET.SubElement(qlr, 'maplayer', type="raster", name=filename.replace('.qlr', ''), hasScaleBasedVisibilityFlag="0")
    ET.SubElement(layer_elem, 'id').text = filename.replace('.qlr', '')
    ET.SubElement(layer_elem, 'datasource').text = xyz_url
    ET.SubElement(layer_elem, 'layername').text = filename.replace('.qlr', '')
    ET.SubElement(layer_elem, 'provider').text = 'wms'
    ET.SubElement(layer_elem, 'type').text = 'raster'

    path = os.path.join(tempfile.gettempdir(), filename)
    tree = ET.ElementTree(qlr)
    tree.write(path, encoding='utf-8', xml_declaration=True)
    return path

def upload_to_drive(filepath: str, filename: str, folder_id: str, creds) -> str:
    service = build('drive', 'v3', credentials=creds)
    file_metadata = {
        'name': filename,
        'parents': [folder_id]
    }
    media = MediaFileUpload(filepath, mimetype='application/xml')
    file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    file_id = file.get('id')
    return f"https://drive.google.com/uc?id={file_id}&export=download"
