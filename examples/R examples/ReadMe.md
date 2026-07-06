------------------------------------------------------------------------

# Accessing `terndata.ecoplots` in R via the `reticulate` package

------------------------------------------------------------------------

## Overview

With Python integration via the [reticulate](https://rstudio.github.io/reticulate/ "https://rstudio.github.io/reticulate/") R package, the terndata.ecoplots package can be used from within R Studio to download data directly into R’s working environment.

## Quick start

The following example downloads all plant tissue sample data from the TERN Ecosystem Surveillance dataset for the Northern Territory.

``` r
## Step 1: load libraries     
library(reticulate)     
py_require("terndata.ecoplots")  

## Step 2: select and download data from terndata.ecoplots     
py_run_string(" 

## load package 
from terndata.ecoplots import EcoPlots  

## load sample data into ec 
ec = EcoPlots(mode='samples') 

## select dataset, region_type, region and material_sample_type 
ec.select(dataset='TERN Ecosystem Surveillance',
          region_type='States and Territories',
          region='Northern Territory',
          material_sample_type='Plant Tissue Sample')

## download data as pandas dataframe 
nt_sample_data_py = ec.get_data(dformat='pandas')                
")  

## Step 3: load data into R environment      
## objects from the Python environment can be accessed via "py$"     

nt_sample_data = py$nt_sample_data_py     
## if necessary: R-ify NAs (see additional information below)     
nt_sample_data[nt_sample_data == "N/A"] = NA`]
```

## Additional information

1.  [Running Python from the console]{.underline}: In the script above, the Python code is executed by the reticulate::py_run_string() function. Alternatively, a Python console can be opened in R studio with reticulate::repl_python() which allows for interactive data exploration with terndata.ecoplots. Note, that R Studio does not display the head of the dataframe as intended by the preview() function of terndata.ecoplots, however, the column names can be accessed without downloading the dataset by ec.preview().columns.

2.  [Python to R NA translation]{.underline}: NAs are not automatically translated when loading pandas dataframes into R. R will translate pandas NAs as “N/A” strings - if the pandas dataframe contains NAs, they need to be replaced by R’s native NA notation (see line 24).

3.  [Wide data format]{.underline}: Generally, terndata.ecoplots provides data in wide format. Most visualisation or statistical analysis in R require long format, depending on the data and intended use, reformating might be necessary.

`reticulate` enables the execution of Python code from within the R environment.

After installing the `reticulate` package

``` r
install.pacakges("reticulate")
```

the `terndata.ecoplots` package can be installed

``` r
 py_require("terndata.ecoplots")
```

and the terndata.ecoplots package can be accessed via

``` r
## download sample data
    py_run_string("
from terndata.ecoplots import EcoPlots

## load data 
ec = EcoPlots()

## select dataset and feature type
ec.select(dataset='QBEIS',
          feature_type='soil')

## download data and ensure correct type for conversion to R data frame
soil_data = ec.get_data(dformat='pandas')
soil_data = soil_data.astype(object)
               ")
```
