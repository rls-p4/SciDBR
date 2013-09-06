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


Examples of new and still-developing features
===

## Heatmaps
The package overloads the standard R `image` function to plot heatmaps
of SciDB array objects (only applies to objects of class `scidb`).
```
library("devtools")
install_github("SciDBR","Paradigm4")
library("scidb")
scidbconnect()      # Fill in your SciDB hostname as required

# Create a SciDB array with some random entries
iquery("store(build(<v:double>[i=0:999,100,0,j=0:999,250,0],random()%100),A)")

# The SciDBR `image` function overloads the usual R image function to produce
# heatmaps using SciDB's `regrid` aggregation operator. The 'grid' argument
# specifies the output array size, and the 'op' argument specifies the
# aggregation operator to apply.

X = image(A, grid=c(100,100), op="avg(v)", useRaster=TRUE)
```
![Example output](https://raw.github.com/Paradigm4/SciDBR/master/inst/misc/image.jpg "Example output")

```
# Image accepts all the standard arguments to the R `image` function in
# addition to the SciDB-specific `grid` and `op` arguments. The output axes are
# labeled in the original array units. The scidb::image function returns the
# interpolated heatmap array:

dim(X)
[1] 100 100
```

## Aggregation, merge, and related functions
The package has a completely new implementation of aggregation, merge, and
related database functions. The new functions apply to SciDB array and data
frame-like objects. A still growing list of the functions includes:

* aggregate
* bind  (SciDB `apply` operator--generalizes R's `cbind`)
* filter
* index_lookup
* merge (SciDB `join` and `cross\_join` operators)
* project
* subset
* sort
* unique

Perhaps the coolest new feature associated with the functions listed above
is that they can be composed in a way that defers computation in SciDB to
avoid unnecessary creation of intermediate arrays. The new functions all
accept an argument named `eval` which, when set to `FALSE`, returns a new
SciDB expression object in place of evaluating the query and returning an
array or data frame object. SciDB expression objects have class `scidbexpr`
and all of the new functions accept them as input.
