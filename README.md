# Running python program
- ### Create a virtual environment: In the main directory ./recipes-etl-1. This virtual environment helps in isolating project dependencies.
```
python3.xx -m venv ./recipe-etl-1/$(name of the virtual env)
```

- ### Activate the virtual environment: We activate the virtual environment using source . This step ensures that when dependencies are installed , they are isolated within the virtual environment.
```
source  ./recipe-etl-1/$(name of the virtual env)/bin/activate
```
- ### Install dependencies: We install project dependencies listed in requirements.txt. This installs all the necessary packages and libraries required for the project.
```
cd ./hf_bi_python_exercise
pip install -r requirements.txt
```
- ### Run the main script: We run the main script , this script likely contains the main logic for data processing using PySpark.
```
python ./hf_bi_python_exercise/main.py
```
# To run testcases
```
cd ./hf_bi_python_exercise
pytest Recipe_Processing_Test
```

# Design approach
### 1. Using PySpark for handling large datasets: PySpark is a great choice for processing large datasets due to its distributed computing capabilities.

### 2.	Organizing data into directories by date: By organizing  data into directories based on dates, we're ensuring that each day's data is stored separately, which helps prevent overwriting and makes it easier to manage and analyze historical data.

### 3.	Using Object-Oriented Programming (OOP): Writing  code in an object-oriented manner can improve maintainability, modularity, and extensibility. It allows us to encapsulate functionality into classes and objects, making it easier to manage and understand.


