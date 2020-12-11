## US-Accidents: A Countrywide Traffic Accident Dataset Analysis

This project focuses on understanding the accident trends in the United States from the years 2016 - 2020 by analyzing it with other accident-provoking factors and answering questions like the number of accidents that happened because of low visibility and the severity of them, finding accident-prone zones on busy streets in major cities, etc. 

### Features

- Most and least accident-prone states in the US
- Accident count per state using severities from low to high in the US
- Accident count per year in the US
- Accident count per month of the year in the US
- Accident count per day of the week in the US
- Accident count by the hour of the day in the US
- Accidents caused by different weather conditions in different months of the years in the US
- Aggregated monthly accident count in the top twenty cities in the US
- Accidents caused by the severity of levels one and two due to low visibility in the US
- Accidents caused by the severity of levels three and four due to low visibility in the US
- Accidents caused by different weather conditions at various times of the day in Houston
- Accidents caused by different severity levels at various times of the day in Houston
- Twenty most accident-prone streets in Houston
- Accident count due to nearby road features in Houston
- Accident-prone areas in hundred most busy streets in the US

### Directory Structure

    .
    |-- doc                           # Project report(.pdf), project proposal(.txt) and figures(.png)
    |    |-- figs                           # Expected 15 output graphs on Dash(UI)
              |-- Visual Graphs                    # 15 (.png) files       
         |-- Project Proposal               # Proposed project proposal
    |-- src                           # CSV files and code
    |    |-- csv_files                      # 16 CSV files generated after running the pyspark code 
         |-- code files(.py)                # python files for generating (.csv) files and the UI
    |__ README.md                     # repository's readme

### Notes
This project is submitted as the final project for CMPT 732: Programming for Big Data Lab 1.
- Code running instructions can be found in RUNNING.MD
- Project details can be found in report.pdf

### Demo
- Live demo for UI Design can be found at: https://usa-accidents.herokuapp.com/
- Video for the project can be found at:

#### Contributors

- Akash Sindhu
- Bilal Hussain
- Sakina Patanwala
