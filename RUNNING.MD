### Prerequisites:  
`python -m venv env`  
`env/Scripts/activate`  
`pip install -r requirements.txt`




STEP 1: Compilation steps for producing dataframes for generating csv files

Step 1: PART A:
- Download the Kaggle Datset from [here](https://www.kaggle.com/sobhanmoosavi/us-accidents)  and store into `Cmpt_732_big_data_I_final_project/data/US_Accidents_June20.csv`.  

- `cd Cmpt_732_big_data_I_final_project/src/`  

- `spark-submit PYSPARK_GRAPH_1_7.py /path_to_data /output_path`

- `spark-submit q3_blind_spot.py /path_to_data` (output will be saved into folders `output_city`, `output_city_hr`, `output_accident_prone`.


- For example: `spark-submit PYSPARK_GRAPH_1_7.py /Users/patan/Desktop/Kaggle_us.csv /Users/Desktop/Plots` and `spark-submit q3_blind_spot.py /Users/Desktop/Kaggle_us.csv`


Step 1: PART B:
- Download the (.csv) file from `CSV_GRAPH_1_7` in the "src" directory in the github repository
- The output (.csv) file from `q3_blind_spot.py` produces a dataframe with one column containing a list of sets of tuples. Inorder to plot those on a graph we had to separate the column with tuples into a list of latitudes and longitudes along with the city and street name. 
- The `blind_spot_lat_long_extraction.py` file does exactly that. 
- Provide the path of the (.csv) file from `q3_blind_spot.py` as an input file to `blind_spot_lat_long_extraction.py` which will in return produce a (.csv) file for usage in "STEP 2"

STEP 2: Compilation steps for producing UI:
- Compile by using Command Prompt using command `python UI_GRAPH_1_7.py`

- It will show the localhost, you can acess the Web based UI from there or click [here](https://usa-accidents.herokuapp.com/)
