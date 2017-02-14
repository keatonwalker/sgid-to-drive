from __future__ import print_function
from apiclient import errors
from apiclient.http import MediaFileUpload
import httplib2
import os
import hashlib

from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

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


def update_file(service, file_id, new_title, new_description, new_mime_type,
                new_filename):
  """Update an existing file's metadata and content.

  Args:
    service: Drive API service instance.
    file_id: ID of the file to update.
    new_title: New title for the file.
    new_description: New description for the file.
    new_mime_type: New MIME type for the file.
    new_filename: Filename of the new content to upload.
    new_revision: Whether or not to create a new revision for this file.
  Returns:
    Updated file metadata if successful, None otherwise.
  """
  try:
    # First retrieve the file from the API.
    file = service.files().get(fileId=file_id, fields='name, description').execute()
    #new_mime_type = 'application/vnd.google-apps.unknown'#file['mimeType']
    # File's new metadata.
    file['name'] = new_title
    file['description'] = new_description
    #file['mimeType'] = new_mime_type

    # File's new content.
    media_body = MediaFileUpload(
        new_filename, mimetype=new_mime_type, resumable=True)

    # Send the request to the API.
    # updated_file = service.files().update(
    #     fileId=file_id,
    #     body=file).execute()
    updated_file = service.files().update(
        fileId=file_id,
        body=file,
        media_body=media_body).execute()
    return updated_file
  except errors.HttpError, error:
    print('An error occurred: {}'.format(error))
    return None


def update_revision(service, file_id, revision_id):
  """Pin a revision.

  Args:
    service: Drive API service instance.
    file_id: ID of the file to update revision for.
    revision_id: ID of the revision to update.

  Returns:
    The updated revision if successful, None otherwise.
  """
  try:
    # First retrieve the revision from the API.
    revision = service.revisions().get(
        fileId=file_id, revisionId=revision_id).execute()
    print(revision)
    # revision['pinned'] = True
    # return service.revisions().update(
    #     fileId=file_id, revisionId=revision_id, body=revision).execute()
  except errors.HttpError, error:
    print('An error occurred: {}'.format(error))
  return None


def retrieve_revisions(service, file_id):
  """Retrieve a list of revisions.

  Args:
    service: Drive API service instance.
    file_id: ID of the file to retrieve revisions for.
  Returns:
    List of revisions.
  """
  try:
    revisions = service.revisions().list(fileId=file_id).execute()
    print(revisions)
    return revisions.get('revisions', [])
  except errors.HttpError, error:
    print('An error occurred: {}'.format(error))
  return None


def main():
    """Shows basic usage of the Google Drive API.

    Creates a Google Drive API service object and outputs the names and IDs
    for up to 10 files.
    """
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v3', http=http)

    fileId = '0B3wvsjTJuTRQMHhWY2JlLW9iSnM'
    new_file = r'./test/data/repos.zip'
    # request = service.files().get_media(fileId=fileId)
    # print(request.to_json())

    #f = service.revisions().list(fileId=fileId).execute()
    # f = service.revisions().get(fileId=fileId, revisionId='0B3wvsjTJuTRQS09JdnhvMkpBRTlSS2NoVXZiRlZETGMyTWdBPQ', fields='originalFilename, size').execute()
    f = service.files().get(fileId=fileId, fields='name, size').execute()
    print(f)
    # file_path = r'/Volumes/C/GisWork/drive_sgid/test_outputs/Trails_gdb.zip'
    # local_file_hash = hashlib.md5(open(file_path, 'rb').read()).hexdigest()
    # print(local_file_hash)
    # upFile = update_file(service, fileId, 'repos.zip', 'ohyeah2', 'application/zip', new_file)
    # print(upFile)

    #revs = retrieve_revisions(service, fileId)
    #print(len(revs))

if __name__ == '__main__':
    main()
