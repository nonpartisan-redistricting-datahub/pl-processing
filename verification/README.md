# al_check

This is code to check the RDH's processed Alabama data against Steven Ochoa's (MALDEF).

We found our processed values to be EQUAL.

If you'd like to run the check yourself, the source files are uploaded here and the validation can be run in the Jupyter Notebook called `alabama_check_ochoa.ipynb`

Note: Due to size constraints, we had to zip together the RDH files, for the notebook to run as is, please unzip `zipped_files.zip` within the `al_rdh_processed_csvs` folder 


# Here's the output of the notebook:

County_al_legacy_formatted_short.csv
both          67
left_only      0
right_only     0
Name: _merge, dtype: int64

There are  67  total rows
0  of these rows have differences
67  of these rows are the same

The max difference between any one shared column in a row is:  0
There are  0 rows with a difference greater than 10

All rows containing differences:
[]

****************************
State_al_legacy_formatted_short.csv
both          1
left_only     0
right_only    0
Name: _merge, dtype: int64

There are  1  total rows
0  of these rows have differences
1  of these rows are the same

The max difference between any one shared column in a row is:  0
There are  0 rows with a difference greater than 10

All rows containing differences:
[]

****************************
Place_al_legacy_formatted_short.csv
both          593
left_only       0
right_only      0
Name: _merge, dtype: int64

There are  593  total rows
0  of these rows have differences
593  of these rows are the same

The max difference between any one shared column in a row is:  0
There are  0 rows with a difference greater than 10

All rows containing differences:
[]

****************************
StateLower_al_legacy_formatted_short.csv
both          105
left_only       0
right_only      0
Name: _merge, dtype: int64

There are  105  total rows
0  of these rows have differences
105  of these rows are the same

The max difference between any one shared column in a row is:  0
There are  0 rows with a difference greater than 10

All rows containing differences:
[]

****************************
StateUpper_al_legacy_formatted_short.csv
both          35
left_only      0
right_only     0
Name: _merge, dtype: int64

There are  35  total rows
0  of these rows have differences
35  of these rows are the same

The max difference between any one shared column in a row is:  0
There are  0 rows with a difference greater than 10

All rows containing differences:
[]

****************************
Congress_al_legacy_formatted_short.csv
both          7
left_only     0
right_only    0
Name: _merge, dtype: int64

There are  7  total rows
0  of these rows have differences
7  of these rows are the same

The max difference between any one shared column in a row is:  0
There are  0 rows with a difference greater than 10

All rows containing differences:
[]

****************************
