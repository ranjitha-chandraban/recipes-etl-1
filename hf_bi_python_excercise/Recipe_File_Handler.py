import requests
import pyspark
import os
import shutil
from datetime import datetime

class RecipeDownloader:
    @staticmethod
    def download_json(url: str) -> str:
        file_url = url

        # Download the file using requests library
        response = requests.get(file_url)

        # Check if the request was successful
        if response.status_code == 200:

            content = response.content.decode('utf-8')

            output_file = "bi_recipes.json"

            # Writing JSON string to JSON file
            with open(output_file, "w") as f:
                f.write(content)
        return output_file
    @staticmethod
    def read_json(filename: str,spark):
        df=spark.read.json(filename)
        return df
    @staticmethod
    def write_csv(filename,spark,df):
        
        dir_path = os.path.dirname(os.path.realpath(__file__))
        # Get the current timestamp
        timestamp = datetime.now()

        # Extract the date part as a string
        date_string = "data/"+timestamp.strftime("%Y-%m-%d")
        dir_path = os.path.join(dir_path,date_string)
        final_path = os.path.join(dir_path, filename)
        # Temp folder to collect the csv coalesced from distributed spark dfs into a single partition
        temp_path = os.path.join(dir_path, 'temp')
        df = df.coalesce(1)
        

        df.write.options(header='True', delimiter='|').mode("overwrite").csv(temp_path)
            
            
        subfiles = os.listdir(temp_path)
        for fname in subfiles:
            if fname.endswith('.csv'):
                name = fname
        
        # copying/renaming the .csv file to the required directory, shutil.move() takes care of existing file/directory and overwrites with the latest file/directory
        shutil.move(os.path.join(temp_path, name), final_path, copy_function = 'copy2')
        shutil.rmtree(temp_path)


    

        