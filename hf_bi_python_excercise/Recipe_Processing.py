from pyspark.sql.functions import  *
import logging

# Create a logger instance
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
class RecipeProcessor:
    def pattern_matching_rows(self,df: DataFrame,column_name: str,pattern: expr):
        """
    This method for filtering the rows of a given PySpark dataframe by matching any given pattern for a particular column.

    Args:
        df (DataFrame): Dataframe 
        column_name (string): column name
        pattern (regex): pattern to match

    Returns:
        return_type: Dataframe"""
        try:
            expr = pattern
            df = df.filter(df[column_name].rlike(expr))
            return df
        except Exception as e:
            logger.info("Error in pattern matching ")
            logger.exception("Error in pattern_matching_rows function " + str(e))
    
    def to_convert_into_minutes(self,df: DataFrame,column_name: str):
        """
        This method converts the given column format"PT4H5M" into minutes

        Args:
            df (DataFrame): Dataframe 
            column_name (string): column name

        Returns:
            return_type: Dataframe"""
        try:
            # Using Regular Expression to extract the times in minutes
            df = df.withColumn("hours", regexp_extract(col(column_name), 'PT(\d+)H', 1).cast('int')).\
                withColumn("minutes",regexp_extract(col(column_name), 'H(\d+)M', 1).cast('int')).\
                withColumn("minutes2",regexp_extract(col(column_name), 'PT(\d+)M', 1).cast('int'))
            # Adds a new column to store the total(prep + cook) time in minutes and drops the column which are not required any further
            df = df.na.fill({'hours':0,'minutes':0,'minutes2':0})
            df = df.withColumn(f"{column_name}totalminutes",col('hours')*60+col('minutes')+col('minutes2'))
            df = df.drop("hours","minutes","minutes2")
            return df
        except Exception as e:
            logger.info("Error in converting column into minutes")
            logger.exception("Error in to_convert_into_minutes function " + str(e))
     
    def add_difficult_level(self,df: DataFrame,new_column: str,column_name: str):
        r"""
        This method adds a new column name "new_col" based on conditions  of the given column_name

        Args:
            df (DataFrame): Dataframe 
            *str (new_column) : Expects the name of the new column("totalminutes" in our case) which captures the difficulty levels.
            *str (column_name) : Expects the name of the column("Difficulty" in our case) which is used for assigning the difficulty level.

        Returns:
            return_type: Dataframe"""
        try:
            df = df.withColumn(new_column,when(col(column_name) > 60 ,"HARD")
                                .when(((col(column_name) >= 30) & (col(column_name) <=60)),"MEDIUM" )
                                .when(col(column_name) < 30,"EASY")
                                .otherwise("UNKNOWN"))
            return df
        except Exception as e:
                logger.info("Error in adding difficulty level")
                logger.exception("Error in add_difficult_level function " + str(e))
    def agg(self, df: DataFrame,new_column: str,column_name: str):
        r"""
        This method adds a new column name "new_column" and aggregated avg for column_name specified as parameters

        Args:
            df (DataFrame): Dataframe 
            *str (new_column) : Expects the name of the new column("Difficulty" in our case) required for grouping.
            *str (column_name) : Expects the name of the column("totalminutes" in our case) required for aggregating and averaging.

        Returns:
            return_type: Dataframe"""
        try:
            grouped_avg = df.groupby(new_column).avg(column_name)
            grouped_avg = grouped_avg.withColumn('Avg_time',lit('AverageTotalTime'))
            grouped_avg = grouped_avg.select(new_column,'Avg_time','avg(totalminutes)')
            return grouped_avg 
        except Exception as e:
                logger.info("Error in aggregating function")
                logger.exception("Error in agg function " + str(e))
        
