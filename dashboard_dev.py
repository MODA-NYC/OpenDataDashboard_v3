import os
import gspread
import pandas as pd

google_credential = os.getenv('GS_CREDENTIALS')
home_path = os.getenv('HOME')
print("gs creds: ", google_credential)
print("home path: ", home_path)

with open(os.path.join(home_path,'/.config/gspread/service_account.json'), 'w') as f:
    f.write(google_credential)

gc = gspread.service_account()
sh = gc.open("ODD_DEV_Source_File")

worksheet = sh.worksheet("_citywide_")
citywide = pd.DataFrame(worksheet.get_all_records())
print(citywide)