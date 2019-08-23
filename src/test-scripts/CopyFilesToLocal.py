import urllib.request
from datetime import datetime
import os
import zipfile

# Filenames
datestamp = datetime.today().strftime('%Y-%m-%d')
print(datestamp)

filepath = os.path.abspath(os.path.join(os.path.dirname( __file__ ), os.pardir, 'data'))
filename = os.path.join(filepath,'all_cricket_matches.zip')

# URLs
url = "https://cricsheet.org/downloads/all.zip"

# Download files to local directory and unzip
try:
    _ = urllib.request.urlretrieve(url,filename=filename)

except Exception as e:
    print("Error downloading file: {}".format(e))

# zip_ref = zipfile.ZipFile(filename, 'r')
# zip_ref.extractall(filepath)
# zip_ref.close()