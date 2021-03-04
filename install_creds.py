import os

google_credential = os.environ['CREDENTIAL']

with open('~/.gdrive_private', 'w') as f:
    f.write(google_credential)
