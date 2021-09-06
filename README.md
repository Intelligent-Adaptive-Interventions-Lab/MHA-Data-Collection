# MHA-Data-Collection
This is eventually going to be a web application that collects the data from the two database automatically from the Mooclet Database and the MHA Django Controller Database.

## Phase 1 
Phase 1 will be a python script that collects all the data and combine them such that they follow a format as defined in [this imagined dataset](http://tiny.cc/mhaimagineddata). 
The csv will be genearted on demand, and no further processing has been added to the data. 

## Phase 2
Phase 2 will be a python script that does all what Phase 1 does but with imputation and further processing included.

## Phase 3
Phase 3 will be a flask web application that supports the visualization of the data in the csv generated in Phase 2.
