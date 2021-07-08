## Version 3.0.5

- Date: 2021-07-09
- Fix for uploading long vectors via as.scidb()

## Version 3.0.4

- Date: 2021-05-26
- Fix for uploading character vectors containing NA values

## Version 3.0.3

- Date: 2021-05-13
- Support for uploading vectors, dense and sparse matrices as temp arrays via `as.scidb`
- Partial fix for CRAN reviewer comments (https://github.com/Paradigm4/SciDBR/issues/213)

## Version 2.0.0

- Big change: all SciDB arrays are presented as data frames
- Greatly simplified the package, improved data transfer performance
- Support for SciDB 16.9 and older versions at least back to 15.7

## Version 1.1-3

- Support for upcoming SciDB 14.12, untested backwards support for
     older releases

- Improved type parsing support, new binary transfer option for data frame
     objecs. This is much faster than UTF-8 transfer from SciDB.

- Support for streaming data from SciDB.

- Option to disable interrupt handling for faster large data transfers.

- Added functions include order, more flexible dist, peek.

## Version 1.1-2

  SCIDB SUPPORT

 - Support for SciDB 14.3, untested support for older SciDB releases

 - Dropping arrays from other R sessions requires a non-default option
      facilitating multiple-user settings.

 - Queries can be canceled with R user interrupts now (for example with
      CTRL + C or ESC).

  IMPROVEMENTS

 - Many improvements to merge and aggregate functions

 - N-d arrays with multiple attributes can now index with `$`

 - Improved R save/load of R data files containing SciDB objects

  NEW FEATURES

 - Support for Paradigm4 glm and a simple glm model matrix builder

 - Support for Paradigm4 truncated SVD routine

 - Added hist, quantile, all.equal, antijoin, a `c` (SciDB concat-like)
      function and many others

## Version 1.1-1

  WINDOWED AGGREGATES

 - The aggregate function now supports moving and fixed window aggregates.

  TLS/SSL ENCRYPTION AND AUTHENTICATION

 - The package now supports TLS/SSL encrypted communication with the
      shim network service and authentication. The only function affected
      by this is `scidbconnect` -- simply supply an SSL port number and
      username and password arguments.

  LAZY EVALUATION

 - Most scidb and scidbdf object can now represent array promises,
      un-evaluated SciDB queries equipped with a result schema and a
      context environment.

  NEW COMPOSABLE OPERATORS

 - Many new functions were introduced that closely follow underlying SciDB
      AFL operators. All of the new functions are composable with lazy
      evaluation of the underlying query using the new `scidbexpr` class.
      Results are only computed and stored by SciDB when required or explicitly
      requested.

  R SPARSE MATRIX SUPPORT

 - The package now maps sparse SciDB arrays to sparse R matrices. Only
      matrices (2-d arrays) with double-precision attributes are supported.
      Sparse array arithmetic uses the new P4 sparse matrix multiply operator
      when available.

  USING RCurl NOW

 - We introduced a dependency on RCurl in order to support SSL and
      authentication with the SciDB shim service.

  NO MORE SciDB NID

 - The package no longer tries to support SciDB arrays with NID dimensions,
      which never really worked anyway. Instead, many functions now take
      advantage of the new SciDB `uniq` and `index_lookup` operators if
      available (>=SciDB 13.6).
      Future package versions will take this further and introduce array
      dimension labeling using the new operators.

  MANY NEW DATABASE-LIKE FEATURES

 - Aggregate was improved, merge sort, unique, index_lookup, and other new
      functions were added. See the vignette for more information.

## Version 1.1-0

  SIGNIFICANT BUG FIX:

 - Materializing subsetting operations could return inconsistently ordered data when
      results spanned SciDB array chunks across multiple SciDB instances. Data are now
      returned correctly in such cases.

  OTHER BUG FIXES:

 - Fixed a bug in the processing of the start argument in the as.scidb function.

 - Fixed several bugs in the image function.

  NEW FEATURES:

 - The iquery function now accepts n=Inf to efficiently download all output
      from query at once. The iterative=TRUE option should still be used with
      smaller n values to iterate over large results.

 - The crossprod and tcrossprod functions are now available for SciDB arrays
      and mixed SciDB/R objects.
      
 - A diag function is now available for SciDB matrices, returning result as a new
      SciDB 1-D array (vector).

 - Element-wise exponentiation was implemented for scidb array objects.

 - Implemented a sum function for scidb array objects.
