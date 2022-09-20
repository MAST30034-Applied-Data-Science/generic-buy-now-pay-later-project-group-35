## IMPORTANT NOTE
The script 'download.py' requires a file 'cred.env' in the root directory defining a valid ADP username and password, which can be generated [here](https://adp-access.aurin.org.au/login). 

An example 'cred.env' file is given is 'cred-example.env'. Replace text in <> with the relevant information, and save as 'cred.env'

## Pipeline
1. `download.py`: Downloads external datasets into 'data/tables' directory (see IMPORTANT NOTE first).
2. `1-preprocess.ipynb`: Performs all preprocessing of data
3. `2.x-analysis_y.ipynb`: A collection of notebooks that perform analysis on particular datasets, outputting particular datasets to 'data/curated'
4. `3-final_dataset_w_fraud.ipynb`: Joins all datasets, generating the final dataset in 'data/curated'