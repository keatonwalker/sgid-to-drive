import arcpy
import shutil
import os
import zipfile
import csv
from time import clock
from hashlib import md5
from xxhash import xxh32
import json

from apiclient import errors
from apiclient.http import MediaFileUpload
import httplib2
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/drive-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/drive'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Drive API Python Quickstart'

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None


class DriveFile(object):

    def __init__(self, ftp_path, file_id, parent_id):
        self.path = ftp_path
        self.name = os.path.basename(ftp_path)
        self.file_id = file_id
        self.parent_id = parent_id

    def update_file(self, local_file, drive_service):
        print 'updating {}'.format(self.name)
        media_body = MediaFileUpload(local_file,
                                     mimetype='application/zip',
                                     resumable=True)
        # file = drive_service.files().get(fileId=self.file_id, fields='').execute()
        # print file
        file_metadata = {'name': self.name}

        # request = service.files().create(body=file_metadata,
        #                                  media_body=media_body,
        #                                  fields="id")
        request = drive_service.files().update(fileId=self.file_id,
                                               #body=file_metadata,
                                               media_body=media_body)

        response = None
        while response is None:
            status, response = request.next_chunk()
        #   if status :
        #     print('{} percent {}'.format(name, int(status.progress() * 100)))

        return response.get('id')


def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'drive-python-quickstart.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else:  # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials


def setup_drive_service():
    # get auth
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v3', http=http)

    return service


def _filter_fields(fields):
    '''
    fields: String[]
    source_primary_key: string
    returns: String[]
    Filters out fields that mess up the update logic
    and move the primary be the last field so that we can filter it out of the hash.'''
    new_fields = [field for field in fields if not _is_naughty_field(field)]
    new_fields.sort()

    return new_fields


def _is_naughty_field(fld):
    #: global id's do not export to file geodatabase
    #: removes shape, shape_length etc
    #: removes objectid_ which is created by geoprocessing tasks and wouldn't be in destination source
    #: TODO: Deal with possibility of OBJECTID_* being the OIDFieldName
    return fld.upper().startswith('SHAPE') or fld.upper().startswith('SHAPE_') or fld.startswith('OBJECTID')


def _create_hash(string, salt):
    hasher = xxhash.xxh32(string)
    hasher.update(str(salt))

    return hasher.hexdigest()


def load_path_ids(folder_path_id_json):
    path_id_list = None
    with open(folder_path_id_json, 'r') as json_file:
        path_id_list = json.load(json_file)
    folder_path_drivefiles = {}
    for path_id in path_id_list:
        folder_path_drivefiles[path_id['path']] = DriveFile(path_id['path'], path_id['fileId'], path_id['parentId'])
    return folder_path_drivefiles


def get_unpackaged_drivefiles_by_name(folder_path_drivefiles):
    unpackaged_drivefiles = {}

    def _get_category_name(path):
        path_list = path.split('/')
        cat_index = path_list.index('UTM12_NAD83') + 1
        category = path_list[cat_index]
        return category
    print 'name,path1,path2'
    for ftp_path in folder_path_drivefiles:
        if 'UnpackagedData' in ftp_path and 'OLD' not in ftp_path:
            category_file = _get_category_name(ftp_path).lower() + '|' + os.path.basename(ftp_path).lower()
            # category_file = os.path.basename(ftp_path)
            if category_file not in unpackaged_drivefiles:
                unpackaged_drivefiles[category_file] = folder_path_drivefiles[ftp_path].path
            else:
                print '{},{},{}'.format(category_file, unpackaged_drivefiles[category_file], ftp_path)
                # fix issue with /Volumes/ftp/UtahSGID_Vector/UTM12_NAD83/SOCIETY/UnpackagedData/UDOTMap_CityLocationsz
                print'Duplicate unpackaged name'
                print 'First: {}, {}, {}'.format(category_file, unpackaged_drivefiles[category_file], folder_path_drivefiles[ftp_path].file_id)
                print 'Current: {}, {}, {}'.format(category_file, ftp_path, folder_path_drivefiles[ftp_path].file_id)

    return unpackaged_drivefiles


def zip_folder(folder_path, zip_name):
    zf = zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED)
    for root, subdirs, files in os.walk(folder_path):
        for filename in files:
            if not filename.endswith('.lock'):
                zf.write(os.path.join(root, filename),
                         os.path.relpath(os.path.join(root, filename), os.path.join(folder_path, '..')))
    original_size = 0
    compress_size = 0
    for info in zf.infolist():
        original_size += info.file_size
        compress_size += info.compress_size
    zf.close()
    print '{} Original size: {} KB'.format(zip_name, original_size / 1000)
    print '{} Compresses size: {} KB'.format(zip_name, compress_size / 1000)
    pass


def get_hash_lookup(hash_path, hash_field):
    hash_lookup = {}
    with arcpy.da.SearchCursor(hash_path, [hash_field, 'OID@']) as cursor:
        for row in cursor:
            hash_value, hash_oid = row
            if hash_value not in hash_lookup:
                hash_lookup[hash_value] = hash_oid  # hash_oid isn't used for anything yet
            else:
                'Hash OID {} is duplicate wtf?'.format(hash_oid)

    return hash_lookup


def detect_changes(data_path, fields, past_hashes, output_fc, output_hashes, shape_token=None):
    # past_hashes = get_hash_lookup(hashes_path, hash_field)
    hash_store = output_hashes
    cursor_fields = list(fields)
    attribute_subindex = -1
    cursor_fields.append('OID@')
    if shape_token:
        cursor_fields.append('SHAPE@XY')
        cursor_fields.append(shape_token)
        attribute_subindex = -3

    hashes = {}
    with arcpy.da.SearchCursor(data_path, cursor_fields) as cursor, \
            arcpy.da.InsertCursor(output_fc, fields[:-1]) as ins_cursor, \
            open(hash_store, 'wb') as hash_csv:
            hash_writer = csv.writer(hash_csv)
            hash_writer.writerow(('src_id', 'hash', 'centroidxy'))
            for row in cursor:
                hasher = xxh32()  # Create/reset hash object
                hasher.update(str(row[:attribute_subindex]))  # Hash only attributes first
                if shape_token:
                    shape_string = row[-1]
                    if shape_string:  # None object won't hash
                        hasher.update(shape_string)
                    else:
                        hasher.update('No shape')  # Add something to the hash to represent None geometry object
                # Generate a unique hash if current row has duplicates
                digest = hasher.hexdigest()
                while digest in hashes:
                    hasher.update(digest)
                    digest = hasher.hexdigest()
                oid = row[attribute_subindex]
                hash_writer.writerow((oid, digest, str(row[-2])))
                if digest not in past_hashes:
                    print 'OID {} is an update'.format(oid)


def create_formatted_outputs(output_directory, input_feature, output_name, past_hashes_path, hash_field):
    input_desc = arcpy.Describe(input_feature)
    spatial_ref = input_desc.spatialReference
    geo_type = input_desc.shapeType
    # output_name = input_feature.split('.')[-1]
    # Create output GDB and feature class
    output_gdb = arcpy.CreateFileGDB_management(output_directory, output_name)[0]
    output_fc = arcpy.CreateFeatureclass_management(output_gdb,
                                                    output_name,
                                                    geo_type,
                                                    input_feature,
                                                    spatial_reference=spatial_ref)
    # Create directory to contain shape file
    shape_directory = os.path.join(output_directory, output_name)
    if not os.path.exists(shape_directory):
        os.makedirs(shape_directory)
    output_shape = os.path.join(shape_directory, output_name)
    # Create directory for feature hashes
    hash_directory = os.path.join(output_directory, output_name + '_hash')
    if not os.path.exists(hash_directory):
        os.makedirs(hash_directory)
    hash_store = os.path.join(hash_directory, '{}_hashes.csv'.format(output_name))

    # Cursor through input_feature and do some stuff while creating output
    fields = set([fld.name for fld in arcpy.ListFields(input_feature)]) & \
        set([fld.name for fld in arcpy.ListFields(output_fc)])
    fields = _filter_fields(fields)
    fields.append('SHAPE@')
    fields.append('OID@')
    # sql_clause = (None, 'ORDER BY {}'.format('OBJECTID'))
    # unique_salty_id = 0
    hash_ins_time = clock()
    past_hashes = get_hash_lookup(past_hashes_path, hash_field)
    detect_changes(input_feature, fields, past_hashes, output_fc, hash_store, 'SHAPE@WKT')
    # with arcpy.da.SearchCursor(input_feature, fields, sql_clause=sql_clause) as cursor, \
    #         arcpy.da.InsertCursor(output_fc, fields[:-1]) as ins_cursor, \
    #         open(hash_store, 'wb') as hash_csv:
    #         hash_writer = csv.writer(hash_csv)
    #         hash_writer.writerow(('src_id', 'att_hash', 'geo_hash'))
    #         for row in cursor:
    #             unique_salty_id += 1
    #             src_id = row[-1]
    #             shape = row[-2]
    #             geom_hash_digest = None
    #             if shape:
    #                 shape_wkt = shape.WKT
    #                 geom_hash_digest = _create_hash(shape_wkt, unique_salty_id)
    #             #: create attribute hash
    #             attribute_hash_digest = _create_hash(str(row[:-2]), unique_salty_id)
    #             hash_writer.writerow((src_id, geom_hash_digest, attribute_hash_digest))
    #             ins_cursor.insertRow(row[:-1])
    print 'hash ins time: {}'.format(clock() - hash_ins_time)
    # Create shape file
    arcpy.CopyFeatures_management(output_fc, output_shape)

    return (output_gdb, shape_directory, hash_directory)


if __name__ == '__main__':
    # # ************ create zip from sde section
    output_directory = r'C:\GisWork\drive_sgid\test_outputs'
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    else:
        shutil.rmtree(output_directory)
        print 'directory removed'
        os.makedirs(output_directory)
    input_feature = r'Database Connections\Connection to sgid.agrc.utah.gov.sde\SGID10.Recreation.Trails'
    output_name = input_feature.split('.')[-1]
    fc_directory, shape_directory, hash_directory = create_formatted_outputs(output_directory,
                                                                             input_feature,
                                                                             output_name,
                                                                             past_hashes,
                                                                             )

    # # Zip up outputs
    # zip_folder(fc_directory, os.path.join(output_directory, '{}_gdb.zip'.format(output_name)))
    # zip_folder(shape_directory, os.path.join(output_directory, '{}_shp.zip'.format(output_name)))
    # zip_folder(hash_directory, os.path.join(output_directory, '{}_hash.zip'.format(output_name)))

    # ************** read setting stuff from json files section
    # json_file = './data/fileids_20161215_141700.json'
    # path_drivefiles = load_path_ids(json_file)
    # unpackaged_drivefiles = get_unpackaged_drivefiles_by_name(path_drivefiles)
    # print len(unpackaged_drivefiles)

    # ************** update drive file section
    # test_file = DriveFile('fuckerduckery/Trails_gdb.zip',
    #                       '0B3wvsjTJuTRQMHhWY2JlLW9iSnM',
    #                       '0B3wvsjTJuTRQOXNRVTQ2NC1ubFU')
    # drive_service = setup_drive_service()
    #
    # uf = test_file.update_file(r'/Volumes/C/GisWork/drive_sgid/test_outputs/Trails_gdb.zip', drive_service)
    # print uf

    print 'Completed'
