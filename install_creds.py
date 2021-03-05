import os

google_credential = os.getenv('GS_CREDENTIALS')
home_path = os.getenv('HOME')

with open(os.path.join(home_path,'.gdrive_private'), 'w') as f:
    f.write(google_credential)
