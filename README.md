## IMPORTANT NOTE
The script 'download.py' requires a file 'cred.env' in the root directory defining a valid ADP username and password, which can be generated [here](https://adp-access.aurin.org.au/login). 

An example 'cred.env' file is given in 'cred-example.env'. Replace text in <> with the relevant information, and save as 'cred.env'

## Pipeline
1. `download.py`: Downloads external datasets into 'data/tables' directory (see IMPORTANT NOTE first).
2. `1-preprocess.ipynb`: Performs all preprocessing of data
3. `2.x-analysis_y.ipynb`: A collection of notebooks that perform analysis on a given dataset, outputting particular datasets to 'data/curated' for future use.
4. `3-final_dataset_w_fraud.ipynb`: Joins all datasets, generating the final dataset in 'data/curated'

## Changes
1. `3a-combine_datasets.ipynb`: added visualization for variation within each postcode
2. `3b-combine_datasets.ipynb`: added visualization for variation within each postcode
3. `3-final_dataset_w_fraud.ipynb`: combined external data with existing data using chosen postcode data and median method 
                                    (based on 3a and 3b)

TODO: Need to make model