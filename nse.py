
from datetime import datetime, timedelta
import requests
import zipfile, io, os
import pandas as pd


# PRE DEFINED MONTH LIST
MONTHS = [None, "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEPT", "OCT", "OCT", "NOV", "DEC"]

class NSEExtractData:
    
    def __init__(self):
        
        print("Initialize program...")
        # EXTRACTED ZIP AND MODIFIED FILE PATH DIR
        self.extracted_zip_dir = "extracted_csv/"
        self.modified_file_dir = "modified_csv/"
        
        
    def get_equities_url(self, parsed_dates):
                    
        print("Collecting EQUITIES URLs...")
        
        # GET ONE BY ONE DATE FROM generator
        for date in parsed_dates:
            
            # GET DATE AND CREATE FILE NAME
            str_year = str(date[0])
            str_month = date[1]
            file_name = "cm"+date[2]+str_month+str_year+"bhav.csv"
            
            # CREATE DYNAMIC URL FROM GIVEN DATE
            url = "https://archives.nseindia.com/content/historical/EQUITIES/"+str_year+"/"+str_month+"/"+file_name+".zip";
            
            yield url

    def download_zip_and_extract(self, equites_urls):
        
        print("Download and extract zip files...")
        
        # GET ONE BY ONE URL FROM generator
        for url in equites_urls:

            # DOWNLOAD FILE FROM LIVE URL
            r = requests.get(url, stream=True)
            z = zipfile.ZipFile(io.BytesIO(r.content))
            
            # EXTRACT ZIP FILE TO EXTRACTED DIR
            z.extractall(self.extracted_zip_dir)
            
            # GET FILE NAME FROM EXTRACTED FILES
            file_name = url.split("/")[-1].replace(".zip", "")               
            
            print("Extracted file is : ",file_name)
            
            yield file_name
            
    def parse_file_and_copy(self, file_names):
        
        try:
            print("Parse and copy one another...")
            
            # GET FILE NAME FROM EXTRACTED FILES
            for file in file_names:
                
                # CHECK FILE IS EXISTES OR NOT
                if os.path.isfile(self.extracted_zip_dir+file):
                
                    # READ CSV FROM EXTRCTED FILE PATH
                    f = pd.read_csv(self.extracted_zip_dir+file)
                    
                    # COLLECT COLUMN FOR NEW FILE
                    keep_col = ['SYMBOL','SERIES','OPEN','HIGH', 'LOW', 'CLOSE', 'LAST', 'PREVCLOSE', 'TOTTRDQTY', 'TOTTRDVAL', 'TIMESTAMP']
                    
                    # GET COLLECTED COLUMN FROM EXTRACTED FILE
                    new_f = f[keep_col]
                
                    # CHECK MODIFIED FILE PATH IS EXISTS OR NOT
                    if os.path.isdir(self.modified_file_dir) == False:
                        os.mkdir(self.modified_file_dir)
                
                    # COPY MODIFIED FILE TO NEW PATH (MODIDFIED PATH)
                    new_f.to_csv(self.modified_file_dir+file, index=False)
        
            print("Successfully done!!!")
            return True
        
        except Exception as e:
            print("There is some error while parsed and copy file")
            return False
        
    # MAEK DATE LIST FOR LAST 30 DAYS AND AVOID WEEKDAYS
    def get_date_range(self):
        less_one_day = (datetime.now() - timedelta(days=1))
        i = 0
        date_counter = 0
        
        while(True):
            
            # SUBSTRACT ONE DATE FROM CURRENT DATE
            date = less_one_day - timedelta(i)

            # CHECK DATE IS WEEKDAY OR NOT
            if (date.weekday() < 5):
                date_counter += 1
                yield [date.year, MONTHS[date.month], date.strftime('%d')]
            
            i += 1
            
            # LAST 30 DAYS DATE COUNTER
            if date_counter >= 30:
                break
        
# MAKE CLASS OBJECT
nse_obj = NSEExtractData()

# CALL CLASS FUNCTION get_date_range() USING CLASS OBJECT
# NOTE : IF HOLIDAY THEN WE NOT GET FILE FROM URL SO CODE IS BREAK SO JUST I HAVE COMMENTS BELOW CODE
# dates = nse_obj.get_date_range()
# print('dates: ', list(dates))

# REMOVED 10 MAR AND 21 FEB (BECAUSE BOTH OF HOLIDAY) BELOW DATE LIST IS GENERATE DYNAMIC AND I HAVE USE HERE STATIC FOR RMEOVE HOLIDAY
dates =  [[2020, 'MAR', '17'], [2020, 'MAR', '16'], [2020, 'MAR', '13'], [2020, 'MAR', '12'], [2020, 'MAR', '11'], [2020, 'MAR', '09'], [2020, 'MAR', '06'], [2020, 'MAR', '05'], [2020, 'MAR', '04'], [2020, 'MAR', '03'], [2020, 'MAR', '02'], [2020, 'FEB', '28'], [2020, 'FEB', '27'], [2020, 'FEB', '26'], [2020, 'FEB', '25'], [2020, 'FEB', '24'], [2020, 'FEB', '20'], [2020, 'FEB', '19'], [2020, 'FEB', '18'], [2020, 'FEB', '17'], [2020, 'FEB', '14'], [2020, 'FEB', '13'], [2020, 'FEB', '12'], [2020, 'FEB', '11'], [2020, 'FEB', '10'], [2020, 'FEB', '07'], [2020, 'FEB', '06'], [2020, 'FEB', '05']]

# CALL CLASS FUNCTION get_equities_url() USING CLASS OBJECT
equites_urls = nse_obj.get_equities_url(dates)

# CALL CLASS FUNCTION download_zip_and_extract() USING CLASS OBJECT
file_names = nse_obj.download_zip_and_extract(equites_urls)

# CALL CLASS FUNCTION parse_file_and_copy() USING CLASS OBJECT
nse_obj.parse_file_and_copy(file_names)

