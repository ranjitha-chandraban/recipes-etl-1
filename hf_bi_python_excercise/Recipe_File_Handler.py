import requests
import pyspark
import os
import sys
import shutil
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import logging

# Create a logger instance
logger = logging.getLogger(__name__)
class RecipeDownloader:
    r"""Abstact class which contains all the modules requried for data extraction and writing."""
    
    @staticmethod
    def download_json(url: str) -> str:
        r"""
            
            Class method for downloading the "*.json" file and writing it to a new "*.json" file in the local directory.  
    
            Returns the output "*.json" file.
    
            Args:
                *str (url) : Expects the URL of the "*.json" object containing the recipe details.
                
        """
        try:
            file_url = url
            # Download the file using requests library
            response = requests.get(file_url)

            # Check if the request was successful
            if response.status_code == 200:
                content = response.content.decode('utf-8')
                output_file = "bi_recipes.json"
                logger.info("Reading Input Json File from API")
                with open(output_file, "w") as f:
                    f.write(content)
                logger.info("Input json file stored successfully")
                return output_file
        except Exception as e:
            logger.info("Failed to Read Input File!!!")
            logger.exception("Error in download_json function " + str(e))
    @staticmethod
    def read_json(filename: str,spark: SparkSession):
        r"""
            
            Class method for loading the recipe details from the local "*.json" file into a PySpark dataframe.  
    
            Returns the output PySpark object.
    
            Args:
                *str (filename) : Expects the name of the local "*.json" file containing the recipe details.
                *session (spark) : Expects the spark session object which is instantiated in the main file.
                
        """
        try:
            logger.info("Reading input json file from local path")
            df=spark.read.json(filename)
            logger.info("The json file is read into pyspark df")
            return df
        except Exception as e:
            logger.info("The input file is not read from localpath")
            logger.exception("Error in read_json function " + str(e))
        
    
    @staticmethod
    def write_csv(filename: str,spark: SparkSession,df: DataFrame):
        r"""
            
            Class method for writing the transformed data into "*.csv" files inside the "data/Today's_date" folder.  
    
            Args:
                *str (filename) : Expects the name of the "*.csv" which is written in the "data/Today's_date" folder.
                *session (spark) : Expects the spark session object which is instantiated in the main file.
                *DataFrame (df) : Expects a dataframe object after all the required transformations are applied.
                
        """
        try:
            dir_path = os.path.dirname(os.path.realpath(__file__))
            # Get the current timestamp
            timestamp = datetime.now()

            # Extract the date part as a string
            date_string = "data/"+timestamp.strftime("%Y-%m-%d")
            dir_path = os.path.join(dir_path,date_string)
            final_path = os.path.join(dir_path, filename)
            
            # Temp folder to collect and write the ".csv" objects coalesced from distributed spark dfs into a single partition
            temp_path = os.path.join(dir_path, 'temp')
            df = df.coalesce(1)
            df.write.options(header='True', delimiter='|').mode("overwrite").csv(temp_path)
                
            # copying/renaming the .csv file to the required directory, shutil.move() takes care of existing file/directory and overwrites with the latest file/directory
            subfiles = os.listdir(temp_path)
            for fname in subfiles:
                if fname.endswith('.csv'):
                    name = fname
            shutil.move(os.path.join(temp_path, name), final_path, copy_function = 'copy2')
            shutil.rmtree(temp_path)
        except Exception as e:
            logger.info("The file is not written into data folder")
            logger.exception("Error in write_csv method " + str(e))
        