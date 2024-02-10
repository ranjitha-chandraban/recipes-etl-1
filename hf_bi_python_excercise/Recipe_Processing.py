from pyspark.sql.functions import  *
class RecipeProcessor:
    def pattern_matching_rows(self,df,col,pattern):
        expr = pattern
        df = df.filter(df[col].rlike(expr))
        return df
    
    def to_convert_into_minutes(self,df,column_name):
        df = df.withColumn("hours", regexp_extract(col(column_name), 'PT(\d+)H', 1).cast('int')).\
            withColumn("minutes",regexp_extract(col(column_name), 'H(\d+)M', 1).cast('int')).\
            withColumn("minutes2",regexp_extract(col(column_name), 'PT(\d+)M', 1).cast('int'))
        df.select('hours','minutes','minutes2',column_name).show()
        df = df.na.fill({'hours':0,'minutes':0,'minutes2':0})
        df = df.withColumn(f"{column_name}totalminutes",col('hours')*60+col('minutes')+col('minutes2'))
        df = df.drop("hours","minutes","minutes2")
        return df
     
    def add_difficult_level(self,df,new_col,column_name):

        df = df.withColumn(new_col,when(col(column_name) > 60 ,"HARD")
                               .when(((col(column_name) >= 30) & (col(column_name) <=60)),"MEDIUM" )
                               .when(col(column_name) < 30,"EASY")
                               .otherwise("UNKNOWN"))
        return df
    def agg(self, df,new_col,col_name):
        grouped_avg = df.groupby(new_col).avg(col_name)
        grouped_avg = grouped_avg.withColumn('Avg_time',lit('AverageTotalTime'))
        grouped_avg = grouped_avg.select(new_col,'Avg_time','avg(totalminutes)')
        return grouped_avg  
        
