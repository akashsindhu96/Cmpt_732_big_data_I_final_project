Prerequisites:
- Python with package dash=1.17.0 
- Python with dash-bootstrap-components=0.10.7
- Python with pandas=1.1.4
- Python with pyspark=3.0.1

STEP 0: Create a virtual environment with Anaconda and install all the libraries listed in "requirements.txt" in "src" directory in the github repository.

STEP 1: Compilation steps for producing dataframes for generating csv files

Step 1: PART A:
- Download the Kaggle Datset from "https://www.kaggle.com/sobhanmoosavi/us-accidents" or navigate in "src" to "Kaggle_US.csv" in the github repository
- Navigate in the "src" directory and use python files "PYSPARK_GRAPH_1_7.py" and "q3_blind_spot.py" for compilation
- Use commands "spark-submit PYSPARK_GRAPH_1_7.py <path for dataset> <output path>" and "spark-submit q3_blind_spot.py <path for dataset>" respectively
- For example: "spark-submit PYSPARK_GRAPH_1_7.py /Users/patan/Desktop/Kaggle_us.csv /Users/patan/Desktop/Plots" and "spark-submit q3_blind_spot.py /Users/patan/Desktop/Kaggle_us.csv"
- CSV files should be produced in inside the <output path> with 13 and 1 (.csv) files for "PYSPARK_GRAPH_1_7.py" and "q3_blind_spot.py" respectively

Step 1: PART B:
- Download the (.csv) file from "CSV_GRAPH_1_7" in the "src" directory in the github repository
- The output (.csv) file from "q3_blind_spot.py" produces a dataframe with one column containing a list of sets of tuples. Inorder to plot those on a graph we had to separate the column with tuples into a list of latitudes and longitudes along with the city and street name. 
- The "blind_spot_lat_long_extraction.py" file does exactly that. 
- Provide the path of the (.csv) file from "q3_blind_spot.py" as an input file to "blind_spot_lat_long_extraction.py" which will in return produce a (.csv) file for usage in "STEP 2"

STEP 2: Compilation steps for producing UI:
- Provide the path of the 14 (.csv) files produced after following "STEP 1" in the "UI_GRAPH_1_7.py" file found in the "src" directory 
- Compile by using Command Prompt using command "python UI_GRAPH_1_7.py"

Notes: Please make sure to install all the required libraries and adjust path for (.csv) files.

Step 2: PART A: Steps for cloning the repository on the local system
- Clone the repository into the local system
- Change directory to "src"
- Inside the "src" folder, there should be a python file called "UI_GRAPH_1_7.py" and also a directory called "CSV_GRAPH_1_7.py" which holds all the (.csv) files obtained after following "STEP 1"
- While inside the parent folder (the original directory of the cloned github repository) simply open the command prompt and use the command "python UI_GRAPH_1_7.py" to run the UI and obtain the temporary URL from Dash to start a local instance of the UI
- One can also see a live demo of the UI here: https://usa-accidents.herokuapp.com/