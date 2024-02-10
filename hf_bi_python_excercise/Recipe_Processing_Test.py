import pytest
from pyspark.sql import SparkSession
from Recipe_Processing import *

# Fixture to create a SparkSession
@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder \
        .master("local") \
        .appName("Recipe_Processor_Test") \
        .getOrCreate()

# Test function
def test_pattern_matching_rows(spark_session: SparkSession):
    """To test a function which  rows of  a particular column that matches the specified pattern """
    # Create a test DataFrame
    data = [("This is testing Chilies",),
        ("Chily",),
        ("This is not ch",),
        ("this is Cilantro",),
        ("It's Chils Powder",)]

# Define the schema for the DataFrame
    schema = ["Ingredients"]
    df = spark_session.createDataFrame(data, schema)

    # Call the function 
    rp = RecipeProcessor()
    result_df = rp.pattern_matching_rows(df,"Ingredients",r'(?i)\b[c]{1,2}[h]{0,2}[i1!l]{1,2}[l1!]{1,2}[yei]{1,10}[s]{0,2}\b')

    # Check the result
    data = [("This is testing Chilies",),
        ("Chily",)]
    schema = ["Ingredients"]
    expected_df = spark_session.createDataFrame(data, schema)
    assert result_df.collect() == expected_df.collect()
    
def test_convert_into_minutes(spark_session: SparkSession):
    """To test on converting a column containing format "PT5H4M" data into  minutes with a new column name """
    data = [("PT5M",),
        ("PT1H5M",),
        ("PT15M",),
        ("PT35M",),
        ("PT3H5M",)]

# Define the schema for the DataFrame
    schema = ["prepTime"]
    df = spark_session.createDataFrame(data, schema)

    # Call the function
    rp = RecipeProcessor()
    result_df = rp.to_convert_into_minutes(df,"prepTime")
    result_df.show(truncate=False)
    # Check the result
    data = [("PT5M", 5),
        ("PT1H5M",65),
        ("PT15M",15),
        ("PT35M",35),
        ("PT3H5M",185)]
    schema = ["prepTime","prepTimetotalminutes"]
    expected_df = spark_session.createDataFrame(data, schema)
    assert result_df.collect() == expected_df.collect()

def test_add_difficult_level(spark_session: SparkSession):
    """To test on adding a column named difficulty values as "HARD,MEDIUM,EASY" based on totalminutes column conditions"""
    data = [(45,),
        (68,),
        (35,),
        (29,),
        (30,)]

# Define the schema for the DataFrame
    schema = ["totalminutes"]
    df = spark_session.createDataFrame(data, schema)

    # Call the function
    rp = RecipeProcessor()
    result_df = rp.add_difficult_level(df,"Difficulty","totalminutes")
    result_df.show(truncate=False)
    # Check the result
    data = [(45, "MEDIUM"),
        (68,"HARD"),
        (35,"MEDIUM"),
        (29,"EASY"),
        (30,"MEDIUM")]
    schema = ["Difficulty","totalminutes"]
    expected_df = spark_session.createDataFrame(data, schema)
    assert result_df.collect() == expected_df.collect()
    
