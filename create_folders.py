from __future__ import print_function
import os
from apiclient import errors
from apiclient.http import MediaFileUpload
import httplib2
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage
import json
from time import strftime
import shutil
import csv

unique_run_num = strftime("%Y%m%d_%H%M%S")

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/drive-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/drive'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Drive API Python Quickstart'


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
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials


def file_exists(file_id):
    """
    Checks whether a file exists on the Drive and is not trashed.
    :param fileId: The ID of the file to check.
    :type fileId: str
    :returns: bool
    """
    if not file_id:
        return False
    try:
        results = file_service.get(fileId=file_id, fields="trashed").execute()
        # Return False if the file is either trashed or does not exist
        return not results['trashed']
    except Exception:
        return False


def get_file_id_name_and_directory(name, parent_id, service):
    response = service.files().list(q="name='{}' and '{}' in parents".format(name, parent_id),
                                    spaces='drive',
                                    fields='files(id)').execute()
    files = response.get('files', [])
    if len(files) > 0:
        return files[0].get('id')
    else:
        return None


def create_drive_folder(service, parent_id, name):
    file_metadata = {
      'name': name,
      'mimeType': 'application/vnd.google-apps.folder',
      'parents': [parent_id]
    }
    folder = service.files().create(body=file_metadata,
                                    fields='id').execute()
    return folder.get('id')


def create_drive_file(service, parent_id, name, media_body):

    file_metadata = {'name': name,
                     'mimeType': 'application/zip',
                     'parents': [ parent_id ]}

    request = service.files().create(body=file_metadata,
                                        media_body=media_body,
                                        fields="id")
    response = None
    while response is None:
      status, response = request.next_chunk()
    #   if status :
    #     print('{} percent {}'.format(name, int(status.progress() * 100)))

    return response.get('id')


def copy_directory_structure_to_drive(root_drive_folder_id, top_level_directory, folder_path_id_json, service):
    root_drive_folder = root_drive_folder_id
    top_dir = top_level_directory
    google_folder_ids = {}
    google_folder_ids[top_dir] = root_drive_folder
    total_folders = 0
    for root, dirs, files in os.walk(top_dir, topdown=True):
        for name in dirs:
            dir_path = os.path.join(root, name)
            if dir_path not in google_folder_ids:
                temp_id = create_drive_folder(service, google_folder_ids[root], name)
                google_folder_ids[dir_path] = temp_id
                total_folders += 1
                if total_folders % 10 == 0:
                    print('Created folder count: {}'.format(total_folders))

    path_id_list = []
    for ftp_path in google_folder_ids:
        path_id_list.append({
            'path': ftp_path,
            'fileId': google_folder_ids[ftp_path],
            'parentId': google_folder_ids.get(os.path.dirname(ftp_path), '')
        })
    with open(folder_path_id_json, 'w') as folder_ids:
        json.dump(path_id_list, folder_ids)
    print('created folder id json: {}'.format(folder_path_id_json))
    print('Total folders created: {}'.format(total_folders))


def load_path_ids(folder_path_id_json):
    path_id_list = None
    with open(folder_path_id_json, 'r') as json_file:
        path_id_list = json.load(json_file)
    folder_path_ids = {}
    for path_id in path_id_list:
        folder_path_ids[path_id['path']] = path_id['fileId']
    return folder_path_ids


def load_all_zip_files(top_level_directory, folder_path_id_json, service, file_path_json):
    folder_path_ids = load_path_ids(folder_path_id_json)
    top_dir = top_level_directory
    google_file_ids = {}
    total_files = 0
    for root, dirs, files in os.walk(top_dir, topdown=True):
        for name in files:
            if name.endswith('.zip'):
                parent_id = folder_path_ids[root]
                dir_path = os.path.join(root, name)
                file_size = os.path.getsize(dir_path)
                if file_size > 600000000:
                    print('skipping: {} size: {} MB'.format(dir_path, file_size / 1000000.0))
                    continue
                elif file_size < 600000000 and file_size > 100000000:
                    print('Loading large file {} size: {}'.format(dir_path, file_size / 1000000.0))

                existing_file_id = get_file_id_name_and_directory(name, parent_id, service)
                if existing_file_id:
                    google_file_ids[dir_path] = existing_file_id
                else:
                    try:
                        media_body = MediaFileUpload(dir_path,
                                                     mimetype='application/zip',
                                                     resumable=True)
                        file_id = create_drive_file(service, parent_id, name, media_body)
                        google_file_ids[dir_path] = file_id
                    except:
                        print('Failed: {}'.format(dir_path))
                        continue

                total_files += 1
                if total_files % 10 == 0:
                    print('Created file count: {}'.format(total_files))

    path_id_list = []
    for ftp_path in google_file_ids:
        path_id_list.append({
            'path': ftp_path,
            'fileId': google_file_ids[ftp_path],
            'parentId': folder_path_ids.get(os.path.dirname(ftp_path), '')
        })
    with open(file_path_json, 'w') as file_ids:
        json.dump(path_id_list, file_ids)
    print('created file id json: {}'.format(file_path_json))
    print('Total files created: {}'.format(total_files))


def load_all_zip_files_test(service):
    top_level_directory = './test/dir_structure'
    folder_path_id_json = './test/data/folderids_test.json'
    file_path_id_json = './test/data/fileids_test.json'
    load_all_zip_files(top_level_directory, folder_path_id_json, service, file_path_id_json)


def print_excluded_zip_files(top_level_directory, file_path_id_json):
    file_path_ids = load_path_ids(file_path_id_json)
    print(len(file_path_ids))
    for root, dirs, files in os.walk(top_level_directory, topdown=True):
        for name in files:
            if name.endswith('.zip'):
                dir_path = os.path.join(root, name)
                if dir_path not in file_path_ids:
                    print()
                    print(name)
                    print(dir_path)


def create_zip_downloadlink_csv(file_path_id_json, output_directory):
    link_csv = os.path.join(output_directory, 'ftp_file_downloadlinks_{}.csv'.format(unique_run_num))
    file_path_ids = load_path_ids(file_path_id_json)
    links_created = 0
    with open(link_csv, 'wb') as downloadlinks:
        out_csv = csv.writer(downloadlinks)
        out_csv.writerow(('file name', 'ftp path', 'drive id', 'download link', 'file size(MB)'))
        for path in file_path_ids:
            file_name = os.path.basename(path)
            ftp_path = path
            drive_id = file_path_ids[path]
            download_link = 'https://drive.google.com/uc?export=download&id={}'.format(drive_id)
            file_size = os.path.getsize(path) / 1000000.0
            out_csv.writerow((file_name, ftp_path, drive_id, download_link, file_size))
            links_created += 1
    print('links created: {}'.format(links_created))
    return link_csv


if __name__ == '__main__':
    # get auth
    # credentials = get_credentials()
    # http = credentials.authorize(httplib2.Http())
    # service = discovery.build('drive', 'v3', http=http)

    # root_drive_folder = '0ByStJjVZ7c7mT3lsOXVGVnJvd1E'
    # top_level_directory = r'/Volumes/ftp/UtahSGID_Vector'
    # folder_path_id_json = r'./data/folderids_{}.json'.format(unique_run_num)
    # copy_directory_structure_to_drive(root_drive_folder, top_level_directory, folder_path_id_json, service)

    #top_level_directory = r'/Volumes/ftp/UtahSGID_Vector'
    # folder_path_id_json = './data/folderids_20161215_111017.json'
    # file_path_id_json = './data/fileids_{}.json'.format(unique_run_num)
    # load_all_zip_files(top_level_directory, folder_path_id_json, service, file_path_id_json)

    file_path_id_json = './data/fileids_20161215_141700.json'
    # print_excluded_zip_files(top_level_directory, file_path_id_json)
    create_zip_downloadlink_csv(file_path_id_json, './data')
