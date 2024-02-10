Running python program
•	Create a virtual environment: Using python3.xx -m venv command,  create a virtual environment in the main directory ./recipes-etl. This virtual environment helps in isolating project dependencies.

•	Activate the virtual environment: We activate the virtual environment using source "$path/to/venv/bin/activate". This step ensures that when dependencies are installed , they are isolated within the virtual environment.

•	Install dependencies: We install project dependencies listed in requirements.txt using pip install -r requirements.txt. This installs all the necessary packages and libraries required for the project.

•	Run the main script: We run the main script using python ./hf_bi_python_exercise/main.py This script likely contains the main logic for data processing using PySpark.

Design approach
1.	Using PySpark for handling large datasets: PySpark is a great choice for processing large datasets due to its distributed computing capabilities.

2.	Organizing data into directories by date: By organizing  data into directories based on dates, we're ensuring that each day's data is stored separately, which helps prevent overwriting and makes it easier to manage and analyze historical data.

3.	Using Object-Oriented Programming (OOP): Writing  code in an object-oriented manner can improve maintainability, modularity, and extensibility. It allows us to encapsulate functionality into classes and objects, making it easier to manage and understand.

To run testcases
pytest Recipe_Processing_Test