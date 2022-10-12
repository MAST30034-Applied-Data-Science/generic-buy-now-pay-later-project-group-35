## IMPORTANT NOTES ABOUT DOWNLOAD.PY
The script 'download.py' requires a file 'cred.env' in the root directory defining a valid ADP username and password, which can be generated [here](https://adp-access.aurin.org.au/login). 

An example 'cred.env' file is given in 'cred-example.env'. Replace text in <> with the relevant information, and save as 'cred.env'

Additionally, certain issues regarding relative directories were encountered during this project, likely due to differing setups to access the jupyter notebooks used in this repository. As such, if any 'No such file or directory' errors occur when running `download.py`, one should try changing line 24 of the download script to set `RELATIVE_PATH_TOGGLE` to True, if set to False or vice versa.

## Summary Notebook
The notebook `notebooks/6-summary.ipynb` contains an overview of the key findings, results, assumptions, and issues of this project (roughly 5 minute read). The ideas contained in the summary notebook are explored in greater detail in the respective individual notebooks. Note that certain code blocks inside the summary notebook will rely on the individual notebooks being run first.

## Pipeline
1. `scripts/download.py`: Downloads external datasets into 'data/tables' directory (see IMPORTANT NOTE first).
2. `notebooks/1-preprocessing.ipynb`: Performs all preprocessing of data
3. `notebooks/2.x-analysis_<dataset>.ipynb`: A collection of notebooks that perform analysis on a given dataset, outputting particular datasets to 'data/curated' for future use.
4. `notebooks/3-final_dataset_w_fraud.ipynb`: Joins all datasets, generating the final dataset in 'data/curated' and more.
5. `notebooks/3.1-postcode_join_test_ratio.ipynb` and `notebooks/3.2-postcode_join_test_no_ratio.ipynb`: Showcases comparison between the two choices of SA2 to postcode datasets, with the non-ratio dataset eventually being the one utilised. Note that these notebooks do not actually need to be run, and remain purely for interest.
6. `notebooks/4.x-modelling_<model_name>.ipynb`: Train the 3 time series regression models used in the ranking, predicting values for all months of 2023. Note that `notebooks/4.4-modelling-merch-demo.ipynb` does not build a time series regression model, but instead prepares customer demographic data for use in the ranking system.
7. `notebooks/5-ranking_system.ipynb`: Ranks merchants using the features obtained from previous notebooks. Note that `notebooks/5.1-ranking_system.ipynb` contains the ranking done for defined 'segments' of merchants.