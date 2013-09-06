Thanks for trying out the SciDB package for R. I hope you enjoy using it.

The current development version of this R package can always be installed
directly from GitHub using the devtools package as follows:

```
library('devtools')
install_github("SciDBR","paradigm4")
```

The SciDB R package requires installation of a simple open-source HTTP network
service called on the computer that SciDB is installed on. This service only
needs to be installed on the SciDB machine, not on client computers that
connect to SciDB from R.  It's available in packaged binary form for supported
SciDB operating systems, and as source code which can be compiled and deployed
on any SciDB installation.

See http://github.com/paradigm4/shim  for source code and installation
instructions.


Examples and new/developing features
===

## Heatmaps
```
library("devtools")
install_github("SciDBR","Paradigm4")
library("scidb")
scidbconnect()      # Fill in your SciDB hostname as required

# Create a SciDB array with some random entries
iquery("store(build(<v:dobule>[i=0:999,100,0,j=0:999,250,0],random()%100),A)")

# The SciDBR `image` function overloads the usual R image function to produce
# heatmaps using SciDB's `regrid` aggregation operator. The 'grid' argument
# specifies the output array size, and the 'op' argument specifies the
# aggregation operator to apply.

X = image(A, grid=c(100,100), op="avg(v)", useRaster=TRUE)
```
![Example output](https://github.com/Paradigm4/SciDBR/raw/inst/misc/image.jpg "Example output")

```
# Image accepts all the standard arguments to the R `image` function in
# addition to the SciDB-specific `grid` and `op` arguments. The output axes are
# labeled in the original array units. The scidb::image function returns the
# heatmap array:

dim(X)
[1] 100 100
```
