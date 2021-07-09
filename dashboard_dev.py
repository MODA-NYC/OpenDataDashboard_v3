import os
import gspread
import pandas as pd

google_credential = os.getenv('GS_CREDENTIALS')
home_path = os.getenv('HOME')
print("gs creds: ", google_credential)
print("home path: ", home_path)

creds_location = os.path.join(home_path,'service_account.json')

with open(creds_location, 'w') as f:
    f.write(google_credential)

gc = gspread.service_account(filename=creds_location)
sh = gc.open("ODD_DEV_Source_File")

worksheet = sh.worksheet("_citywide_")
citywide = pd.DataFrame(worksheet.get_all_records())
print(citywide)