from Recipe_File_Handler import RecipeDownloader
from Recipe_Processing import RecipeProcessor
from pyspark.sql import SparkSession

def get_spark_session():
        spark = SparkSession.builder.master("local[1]").appName('HelloFresh') \
                        .getOrCreate()
        return spark

def main(url: str) -> None:
    # Download recipes from URL
    spark=get_spark_session()
    json_file = RecipeDownloader.download_json(url=url)
    recipes_df = RecipeDownloader.read_json(filename=json_file,spark=spark)
    ##Creating an object for RecipeProcessor
    rp = RecipeProcessor()
    chillies_df = rp.pattern_matching_rows(df=recipes_df,col='ingredients',pattern=r'(?i)\b[c]{1,2}[h]{0,2}[i1!l]{1,2}[l1!]{1,2}[yesi]{0,10}\b')
    chillies_df = rp.to_convert_into_minutes(chillies_df,'cookTime')
    chillies_df = rp.to_convert_into_minutes(chillies_df,'prepTime')
    chillies_df = chillies_df.withColumn('totalminutes',chillies_df['cookTimetotalminutes']+chillies_df['prepTimetotalminutes'])
    chillies_df = rp.add_difficult_level(chillies_df,'Difficulty','totalminutes')
    chillies_df_without_total_minutes = chillies_df.drop("totalminutes","prepTimetotalminutes","cookTimetotalminutes")
    # Drop duplicates and write the file in the root directory
    chillies_df_without_total_minutes = chillies_df_without_total_minutes.dropDuplicates()

    # Groupby difficulty level and compute the average in mins
    chillies_df_grouped_avg = rp.agg(chillies_df, 'Difficulty', 'totalminutes')
    chillies_df_grouped_avg = chillies_df_grouped_avg.drop("totalminutes","prepTimetotalminutes","cookTimetotalminutes")
    
    # Write both the Chilies.csv and the Results.csv
    RecipeDownloader.write_csv("Chilies.csv",spark,chillies_df_without_total_minutes)
    RecipeDownloader.write_csv("Results.csv",spark,chillies_df_grouped_avg)
    print("The process is completed. Please find the files in data folder")
    spark.stop()

if __name__ == '__main__':
      main('https://bnlf-tests.s3.eu-central-1.amazonaws.com/recipes.json')
