import os

google_credential = os.environ['GS_CRDENTIALS']

with open('~/.gdrive_private', 'w') as f:
    f.write(google_credential)
