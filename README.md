# fit-data-extract
Pipline to retreve .FIT files from garmin watch and convert then into .csv files

Steps:
1. Download .fit files
2. In Dir with .fit files run `./file_grouper.sh`
    - Make sure that it has the right time tolerance (e.g. 302 to 300 for 5 min recordings)
3. Make sure each sub dir has the same Unit# in the FIT header
4. Run `fit_to_csv.py` on each created group
