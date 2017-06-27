import arcpy
import shutil
import os
import zipfile
import csv
from time import clock, strftime, sleep
from hashlib import md5
# from xxhash import xxh32
import json
import io
import ntpath

from apiclient import errors
from apiclient.http import MediaFileUpload, MediaIoBaseDownload
import httplib2
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage


HASH_DRIVE_FOLDER = '0B3wvsjTJuTRQZUJXWEhEX3p3d1k'
UTM_DRIVE_FOLDER = '0B3wvsjTJuTRQaGluYVphcUNEREE'
# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/drive-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/drive'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Drive API Python Quickstart'

FEATURE_JSON_FOLDER = r'features'
PACKAGE_JSON_FOLDER = r'packages'

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
    hasher = md5(string)
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
    print '{} Compressed size: {} MB'.format(ntpath.basename(zip_name),
                                             compress_size / 1000000.0)


def unzip(zip_path, output_path):
    with zipfile.ZipFile(zip_path, 'r', zipfile.ZIP_DEFLATED) as zipped:
        zipped.extractall(output_path)


def get_hash_lookup(hash_path, hash_field):
    hash_lookup = {}
    with arcpy.da.SearchCursor(hash_path, [hash_field, 'src_id']) as cursor:
        for row in cursor:
            hash_value, hash_oid = row
            if hash_value not in hash_lookup:
                hash_lookup[hash_value] = hash_oid  # hash_oid isn't used for anything yet
            else:
                'Hash OID {} is duplicate wtf?'.format(hash_oid)

    return hash_lookup


def create_hash_table(data_path, fields, output_hashes, shape_token=None):
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
            open(hash_store, 'wb') as hash_csv:
            hash_writer = csv.writer(hash_csv)
            hash_writer.writerow(('src_id', 'hash', 'centroidxy'))
            for row in cursor:
                hasher = md5()  # Create/reset hash object
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


def detect_changes(data_path, fields, past_hashes, output_hashes, shape_token=None):
    """Detect any changes and create a new hash store for uploading."""
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
    changes = 0
    with arcpy.da.SearchCursor(data_path, cursor_fields) as cursor, \
            open(hash_store, 'wb') as hash_csv:
            hash_writer = csv.writer(hash_csv)
            hash_writer.writerow(('src_id', 'hash', 'centroidxy'))
            for row in cursor:
                hasher = md5()  # Create/reset hash object
                hasher.update(str(row[:attribute_subindex]))  # Hash only attributes
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
                    changes += 1

    print 'Total changes: {}'.format(changes)
    return changes > 0


def create_formatted_outputs(output_directory, input_feature, output_name):
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
    sql_clause = (None, 'ORDER BY {}'.format('OBJECTID'))
    unique_salty_id = 0
    hash_ins_time = clock()
    with arcpy.da.SearchCursor(input_feature, fields, sql_clause=sql_clause) as cursor, \
            arcpy.da.InsertCursor(output_fc, fields[:-1]) as ins_cursor, \
            open(hash_store, 'wb') as hash_csv:
            hash_writer = csv.writer(hash_csv)
            hash_writer.writerow(('src_id', 'att_hash', 'geo_hash'))
            for row in cursor:
                unique_salty_id += 1
                src_id = row[-1]
                shape = row[-2]
                geom_hash_digest = None
                if shape:
                    shape_wkt = shape.WKT
                    geom_hash_digest = _create_hash(shape_wkt, unique_salty_id)
                #: create attribute hash
                attribute_hash_digest = _create_hash(str(row[:-2]), unique_salty_id)
                hash_writer.writerow((src_id, geom_hash_digest, attribute_hash_digest))
                ins_cursor.insertRow(row[:-1])
    # print 'hash ins time: {}'.format(clock() - hash_ins_time)
    # Create shape file
    arcpy.CopyFeatures_management(output_fc, output_shape)

    return (output_gdb, shape_directory, hash_directory)


def create_outputs(output_directory, input_feature, output_name):
    """Create output file GDB and directory with shapefile."""
    # input_desc = arcpy.Describe(input_feature)
    # spatial_ref = input_desc.spatialReference
    # geo_type = input_desc.shapeType
    # output_name = input_feature.split('.')[-1]

    # Create output GDB and feature class
    output_gdb = arcpy.CreateFileGDB_management(output_directory, output_name)[0]
    output_fc = arcpy.CopyFeatures_management(input_feature, os.path.join(output_gdb, output_name))[0]
    # Create directory to contain shape file
    shape_directory = os.path.join(output_directory, output_name)
    if not os.path.exists(shape_directory):
        os.makedirs(shape_directory)
    arcpy.CopyFeatures_management(output_fc, os.path.join(shape_directory, output_name))

    return (output_gdb, shape_directory)


def download_zip(file_id, service, output):
    import shutil
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print "Download %d%%." % int(status.progress() * 100)
    fh.seek(0)
    with open(output, 'wb') as out_zip:
        shutil.copyfileobj(fh, out_zip, length=131072)


def update_file(file_id, local_file, drive_service):
    media_body = MediaFileUpload(local_file,
                                 mimetype='application/zip',
                                 resumable=True)

    # file = drive_service.files().get(fileId=self.file_id, fields='').execute()
    # print file
    # file_metadata = {'name': self.name}

    # request = service.files().create(body=file_metadata,
    #                                  media_body=media_body,
    #                                  fields="id")
    request = drive_service.files().update(fileId=file_id,
                                           media_body=media_body)

    response = None
    while response is None:
        status, response = request.next_chunk()
    #   if status :
    #     print('{} percent {}'.format(name, int(status.progress() * 100)))

    return response.get('id')


def create_drive_zip(name, parent_ids, local_file, service):

    file_metadata = {'name': name,
                     'mimeType': 'application/zip',
                     'parents': parent_ids}

    media_body = MediaFileUpload(local_file,
                                 mimetype='application/zip',
                                 resumable=True)
    request = service.files().create(body=file_metadata,
                                     media_body=media_body,
                                     fields="id")
    response = None
    while response is None:
        status, response = request.next_chunk()
    #   if status :
    #     print('{} percent {}'.format(name, int(status.progress() * 100)))

    return response.get('id')


def get_file_id_by_name_and_directory(name, parent_id, service):
    response = service.files().list(q="name='{}' and '{}' in parents  and explicitlyTrashed=false".format(name,
                                                                                                          parent_id),
                                    spaces='drive',
                                    fields='files(id)').execute()
    files = response.get('files', [])
    if len(files) > 0:
        return files[0].get('id')
    else:
        return None


def create_drive_folder(name, parent_ids, service):
    # existing_file_id = get_file_id_by_name_and_directory(name, parent_ids[0], service)
    # if existing_file_id:
    #     print 'Existing file'
    #     return existing_file_id
        # raise Exception('Drive folder {} already exists at: {}'.format(name, existing_file_id))

    file_metadata = {'name': name,
                     'mimeType': 'application/vnd.google-apps.folder',
                     'parents': parent_ids}

    response = service.files().create(body=file_metadata,
                                      fields="id").execute()

    return response.get('id')


def load_zip_to_drive(spec, id_key, new_zip, parent_folder_ids, service):
    if spec[id_key]:
        update_file(spec[id_key], new_zip, service)
    else:
        temp_id = create_drive_zip(ntpath.basename(new_zip),
                                   parent_folder_ids,
                                   new_zip,
                                   service)
        spec[id_key] = temp_id


def get_category_folder_id(category, parent_id, service):
    category_id = get_file_id_by_name_and_directory(category, parent_id, service)
    if not category_id:
        print 'Creating drive folder: {}'.format(category)
        category_id = create_drive_folder(category, [parent_id], service)

    return category_id


def load_feature_json(json_path):
    with open(json_path, 'r') as json_file:
        feature = json.load(json_file)

    return feature


def save_spec_json(json_path, spec):
    with open(json_path, 'w') as f_out:
        spec['upload_date'] = strftime("%Y_%m_%d")
        f_out.write(json.dumps(spec, sort_keys=True, indent=4))


def valitdate_spec(spec):
    if "" in spec['parent_ids']:
        raise Exception('Invalid spec: {}'.format(spec))


def create_feature_spec_name(feature_class):
    spec_name = '_'.join(feature_class.split('.')[-2:]) + '.json'
    return spec_name


def add_packages_to_feature(sgid_name, package_name):
    empty_spec = os.path.join('features', 'template.json')
    spec_name = '_'.join(sgid_name.split('.')[-2:]) + '.json'
    feature_spec = os.path.join('features', spec_name)

    feature = None
    if not os.path.exists(feature_spec):
        feature = load_feature_json(empty_spec)
        feature['sgid_name'] = sgid_name
        feature['name'] = sgid_name.split('.')[-1]
        feature['category'] = sgid_name.split('.')[-2]
        feature['packages'].append(package_name)
    else:
        feature = load_feature_json(feature_spec)
        if package_name not in feature['packages']:
            feature['packages'].append(package_name)
    save_spec_json(feature_spec, feature)


def update_feature(workspace, feature_name, output_directory, drive_service, load_to_drive=True, force_update=False):
    print '\nStarting feature:', feature_name
    empty_spec = os.path.join('features', 'template.json')
    input_feature_path = os.path.join(workspace, feature_name)
    spec_name = '_'.join(feature_name.split('.')[-2:]) + '.json'
    feature_spec = os.path.join('features', spec_name)

    feature = None
    if not os.path.exists(feature_spec):
        feature = load_feature_json(empty_spec)
        feature['sgid_name'] = feature_name
        feature['name'] = input_feature_path.split('.')[-1]
        feature['category'] = input_feature_path.split('.')[-2]
    else:
        feature = load_feature_json(feature_spec)

    # Check for category folder
    category_id = get_category_folder_id(feature['category'], UTM_DRIVE_FOLDER, drive_service)
    if category_id not in feature['parent_ids']:
        feature['parent_ids'].append(category_id)

    output_name = feature['name']

    # Get the last hash from drive to check changes
    past_hash_directory = os.path.join(output_directory, 'pasthashes')
    hash_field = 'hash'
    past_hash_zip = os.path.join(output_directory, 'TrailsDown.zip')
    past_hash_store = os.path.join(past_hash_directory, output_name + '_hash', output_name + '_hashes.csv')
    past_hashes = None
    if feature['hash_id']:
        download_zip(feature['hash_id'], drive_service, past_hash_zip)
        print 'Past hashes downloaded'
        unzip(past_hash_zip, past_hash_directory)
        past_hashes = get_hash_lookup(past_hash_store, hash_field)
    else:
        past_hashes = {}

    # Check for changes
    # Create directory for feature hashes
    hash_directory = os.path.join(output_directory, output_name + '_hash')
    if not os.path.exists(hash_directory):
        os.makedirs(hash_directory)
    hash_store = os.path.join(hash_directory, '{}_hashes.csv'.format(output_name))
    # Get fields for hashing
    fields = set([fld.name for fld in arcpy.ListFields(input_feature_path)])
    fields = _filter_fields(fields)

    changed = detect_changes(input_feature_path, fields, past_hashes, hash_store, 'SHAPE@WKT')

    packages = []
    if changed or force_update:
        packages = feature['packages']
        # Copy data local
        fc_directory, shape_directory = create_outputs(
                                                     output_directory,
                                                     input_feature_path,
                                                     output_name)

        # Zip up outputs
        new_gdb_zip = os.path.join(output_directory, '{}_gdb.zip'.format(output_name))
        new_shape_zip = os.path.join(output_directory, '{}_shp.zip'.format(output_name))
        new_hash_zip = os.path.join(output_directory, '{}_hash.zip'.format(output_name))

        zip_folder(fc_directory, new_gdb_zip)
        zip_folder(shape_directory, new_shape_zip)
        zip_folder(hash_directory, new_hash_zip)
        # Upload to drive
        if load_to_drive:
            load_zip_to_drive(feature, 'gdb_id', new_gdb_zip, feature['parent_ids'], drive_service)
            print 'GDB loaded'
            load_zip_to_drive(feature, 'shape_id', new_shape_zip, feature['parent_ids'], drive_service)
            print 'Shape loaded'
            load_zip_to_drive(feature, 'hash_id', new_hash_zip, [HASH_DRIVE_FOLDER], drive_service)
            print 'Hash loaded'

        save_spec_json(feature_spec, feature)

    return packages


def update_package(workspace, package_name, output_directory, drive_service):
    print '\nStarting package:', package_name
    spec_name = package_name
    if package_name.find('.json') == -1:
        spec_name += '.json'

    package_spec = os.path.join('packages', spec_name)

    package = None
    if not os.path.exists(package_spec):
        raise Exception('Package spec does not exist at {}'.format(package_spec))
    else:
        package = load_feature_json(package_spec)
    valitdate_spec(package)
    # Check for category folder
    category_id = get_category_folder_id(package['category'], UTM_DRIVE_FOLDER, drive_service)
    category_packages_id = get_category_folder_id('packages', category_id, drive_service)
    drive_folder_id = get_category_folder_id(package['name'], category_packages_id, drive_service)
    if drive_folder_id not in package['parent_ids']:
        package['parent_ids'].append(drive_folder_id)

    package_gdb = arcpy.CreateFileGDB_management(output_directory, package['name'])[0]
    package_shape = os.path.join(output_directory, package['name'])
    os.makedirs(package_shape)

    for feature_class in package['FeatureClasses']:
        spec_name = create_feature_spec_name(feature_class)
        feature_spec = os.path.join('features', spec_name)
        if not os.path.exists(feature_spec):
            update_feature(workspace, feature_class, os.path.join(output_directory, '..'), drive_service)

        spec = load_feature_json(feature_spec)
        if package_name not in spec['packages']:
            spec['packages'].append(package_name.replace('.json', ''))
            save_spec_json(feature_spec, spec)

        feature_output_name = spec['name']
        out_fc_path = os.path.join(package_gdb, feature_output_name)

        shape_directory_path = os.path.join(output_directory, '..', feature_output_name)
        fc_path = os.path.join(shape_directory_path + '.gdb', feature_output_name)
        if os.path.exists(shape_directory_path) and arcpy.Exists(fc_path):
            print feature_class, 'local'
            arcpy.CopyFeatures_management(fc_path,
                                          out_fc_path)

            shutil.copytree(shape_directory_path, os.path.join(package_shape, feature_output_name))

        else:
            print feature_class, 'workspace'
            arcpy.CopyFeatures_management(os.path.join(workspace, feature_class),
                                          out_fc_path)

            s_dir = os.path.join(package_shape, feature_output_name)
            os.makedirs(s_dir)
            arcpy.CopyFeatures_management(os.path.join(workspace, feature_class),
                                          os.path.join(s_dir, feature_output_name))

    # Zip up outputs
    print
    new_gdb_zip = os.path.join(output_directory, '{}_gdb.zip'.format(package['name']))
    new_shape_zip = os.path.join(output_directory, '{}_shp.zip'.format(package['name']))

    zip_folder(package_gdb, new_gdb_zip)
    zip_folder(package_shape, new_shape_zip)
    # Upload to drive
    load_zip_to_drive(package, 'gdb_id', new_gdb_zip, package['parent_ids'], drive_service)
    print 'GDB loaded'
    load_zip_to_drive(package, 'shape_id', new_shape_zip, package['parent_ids'], drive_service)
    print 'Shape loaded'

    save_spec_json(package_spec, package)


def run_features(feature_list_json=None):
    run_all_lists = None
    features = []
    if not feature_list_json:
        for root, subdirs, files in os.walk(FEATURE_JSON_FOLDER):
            for filename in files:
                feature_spec = load_feature_json(os.path.join(root, filename))
                if feature_spec["sgid_name"] != '':
                    features.append(feature_spec["sgid_name"])
            break
    else:
        with open(feature_list_json, 'r') as json_file:
            run_all_lists = json.load(json_file)
            features = run_all_lists["features"]

    packages = []
    for feature in features:
        packages.extend(update_feature(workspace, feature, output_directory, drive_service, load_to_drive=True))
    for package in set(packages):
        update_package(workspace, package, temp_package_directory, drive_service)


def run_packages(package_list_json=None):
    run_all_lists = None
    features = []
    packages_to_check = []
    if not package_list_json:
        for root, subdirs, files in os.walk(PACKAGE_JSON_FOLDER):
            for filename in files:
                packages_to_check.append(filename)
            break
    else:
        with open(package_list_json, 'r') as json_file:
            run_all_lists = json.load(json_file)
            packages_to_check.extend(run_all_lists["packages"])

    for p in packages_to_check:
        if not p.endswith('.json'):
            p += '.json'
        packages_spec = load_feature_json(os.path.join(PACKAGE_JSON_FOLDER, p))
        fcs = packages_spec["FeatureClasses"]
        if fcs != '' or len(fcs) > 0:
            for f in fcs:
                add_packages_to_feature(f, p.replace('.json', ''))
                features.append(f)

    features = set(features)
    packages = []
    for feature in features:
        packages.extend(update_feature(workspace, feature, output_directory, drive_service, load_to_drive=True))
    for package in set(packages):
        update_package(workspace, package, temp_package_directory, drive_service)


def run_feature(sgid_name):
    packages = update_feature(workspace, sgid_name, output_directory, drive_service, load_to_drive=True)
    for package in set(packages):
        update_package(workspace, package, temp_package_directory, drive_service)


if __name__ == '__main__':
    drive_service = setup_drive_service()
    # -------------Set these to test--------------------
    workspace = r'Database Connections\Connection to sgid.agrc.utah.gov.sde'
    # feature_name = 'SGID10.RECREATION.Trailheads'
    output_directory = r'package_temp' # TODO make parameter or global
    temp_package_directory = os.path.join(output_directory, 'output_packages')

    def renew_temp_directory(directory, package_dir):
        if not os.path.exists(directory):
            os.makedirs(temp_package_directory)
        else:
            shutil.rmtree(directory)
            print 'Temp directory removed'
            os.makedirs(package_dir)
    renew_temp_directory(output_directory, temp_package_directory)

    start_time = clock()
    run_feature('SGID10.BIOSCIENCE.Habitat_Bison')
    # packages = []
    # features = ['SGID10.RECREATION.SkiTrails_XC']
    # for feature in features:
    #     packages.extend(update_feature(workspace, feature, output_directory, drive_service, load_to_drive=True))
    # for package in set(packages):
    #     update_package(workspace, package, temp_package_directory, drive_service)

    print '\nComplete!', clock() - start_time
