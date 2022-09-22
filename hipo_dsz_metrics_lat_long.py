import pandas as pd
import numpy as np
import os
import pickle
import gspread

from pyhive import presto
from requests.auth import HTTPBasicAuth

from pyjumbo import Presto
from requests.auth import HTTPBasicAuth
presto = Presto(user_name = 'common-python-etls') 

from typing import Any, Dict, List
from pyjumbo import JumboDataUploader
from pyjumbo.google_sheets import Client as GoogleSheetsClient
from datetime import date, timedelta, datetime


import numpy as np
import pandas as pd
import datetime
import os.path
from os import path
import pyjumbo
import matplotlib.pyplot as plt
import datetime
import pandas as pd
import os
import traceback
import smtplib
import time
import smtplib
import mimetypes
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.text import MIMEText

from requests.auth import HTTPBasicAuth

import seaborn as sns

import pyjumbo
from pyjumbo import Presto
import pyjumbo
from pyjumbo import Presto
import numpy as np
import pandas as pd
import hashlib 
import time
from    datetime  import  date, datetime,timedelta
from itertools import product
from pyhive import presto
import json
import zlib


gc = GoogleSheetsClient()
# sheet_id is shared with the owner of this etl
# sheet_id can be found from the url of the google sheet
res_data: List[Dict[str, Any]] = gc.get_all_records(sheet_id='1NTYQ0E7mVWQzWigghmJMoPGeSVebMoS9NVgsTqrj29U',
                                                    worksheet_title='7')
 df = pd.DataFrame(res_data)
print(df.shape)
df['agg_filter']=df['agg_filter'].astype(int)
df['dszid']=df['dszid'].astype(int)
df['city_name']=df['city_name'].astype(str)
df['hiponame']=df['hiponame'].astype(str)
df['cluster']=df['cluster'].astype(str)
df['AO']=df['AO'].astype(int)
df['MO']=df['MO'].astype(int)
df['CB']=df['CB'].astype(int)
df['OM']=df['OM'].astype(int)
df['OV']=df['OV'].astype(int)
df['nu']=df['nu'].astype(int)
df['lu']=df['lu'].astype(int)
df['ru']=df['ru'].astype(int)
df['lat']=df['lat'].astype(float(10,2))
df['long']=df['long'].astype(float(10,2))

print(df.info())
uploader = JumboDataUploader()
filename = 'hipodsz_metrics_lat_long.parquet'
jumbo_table_name = 'hipodsz_metrics_lat_long'
jumbo_db_name = 'jumbo_external'
uploader.upload_panda_df_as_etl_output(df, filename)
uploader.notify_about_etl_output_shared(filename,
                                        jumbotablename=jumbo_table_name,
                                        jumbodbname=jumbo_db_name,
                                        to_jumbo=True,
                                        to_kafka=False,
                                        gendatatype='ETL_PARTITIONED_DATA')
 
