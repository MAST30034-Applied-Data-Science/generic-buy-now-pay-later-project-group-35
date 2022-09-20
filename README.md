## IMPORTANT NOTE
The script 'download.py' requires a file 'cred.env' in the root directory defining a valid ADP username and password, which can be generated [here](https://adp-access.aurin.org.au/login). 

An example 'cred.env' file is given in 'cred-example.env'. Replace text in <> with the relevant information, and save as 'cred.env'

## Pipeline
1. `download.py`: Downloads external datasets into 'data/tables' directory (see IMPORTANT NOTE first).
2. `1-preprocess.ipynb`: Performs all preprocessing of data
3. `2.x-analysis_y.ipynb`: A collection of notebooks that perform analysis on a given dataset, outputting particular datasets to 'data/curated' for future use.
4. `3-final_dataset_w_fraud.ipynb`: Joins all datasets, generating the final dataset in 'data/curated'

## Changes
1. `download.py`: added `relative_dir` constant
2. `1-preprocess.ipynb`: added preprocessing for postcode ratio dataset + `relative_dir` constant
3. `2.2-analysis_postcode_sa2.ipynb`: add analysis steps for postcode ratio dataset
4. `3a-combine_datasets.ipynb`: uses postcode ratio dataset with weighted averaging to combine all datasets
5. `3b-combine_datasets.ipynb`: uses postcode dataset with normal averaging to combine all datasets

TODO: Need to choose between 3a and 3b and then copy code to `3-final_dataset_w_fraud`