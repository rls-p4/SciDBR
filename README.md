Thanks for trying out the SciDB package for R. I hope you enjoy using it.

Install the package from CRAN with
```
install.packages("scidb")
```

The current development version of the package can be installed directly from
sources on  GitHub using the devtools package as follows (requires an R
development environment  and the R devtools package):
```
devtools::install_github("Paradigm4/SciDBR")
```

Note! The SciDBR package depends on the RCurl R package, which in turn requires
support for the curl library in your operating system. This might mean that
you need to install a libcurl development library RPM or deb package on your
OS. On RHEL and CentOS, this package is usually called `libcurl-devel` and on
Ubuntu it's called `libcurl4-gnutls-dev`.

The SciDB R package requires installation of a simple open-source HTTP network
service called on the computer that SciDB is installed on. This service only
needs to be installed on the SciDB machine, not on client computers that
connect to SciDB from R.  See http://github.com/paradigm4/shim  for source code
and installation instructions.

Developers please note that R CMD check-style unit tests are skipped unless a
system environment variable named SCIDB_TEST_HOST is set to the host name or
I.P. address of SciDB. See the tests directory for test code.

Wiki
===
Check out (and feel free to contribute to) examples in the wiki pages for
this project here:

https://github.com/Paradigm4/SciDBR/wiki/_pages

This project also has a pretty web page on Github here:

https://Paradigm4.github.io/SciDBR


Changes in package version 1.3.0
===

This is a major release that breaks some API compatibility with the previous
package release. It's designed to support SciDB version 15.7 and also tries
to maintain compatibility with previous SciDB releases.

## Labeled coordinates have been removed

The package has dropped all use of `rownames`. Use of `rownames` is of marginal
value and the SciDB package implementation was very inefficient. Similar
functionality can be achieved with `subset` (data.frame-like objects), or
indexing by vectors or other SciDB arrays.

The `colnames` and `names` functions still work when applied to data.frame-like
objects.

## The `subset` function is more powerful and efficient

It generates better-optimized SciDB queries than previous versions. See `?subset`
for details and examples.

## Array subset indexing is more efficient

We removed use of `subarray` to reset array coordinate systems after
subsetting.  Subsets of sparse or dense arrays returned to R are labeled by
their original coordinates.

If you need an array subset to start at the coordinate system origin, use
the new `translate` function. For example:

```r
x <- build("double(i+j)", c(5,5))
y <- x[1:2,2:3]
schema(y)          # Note the coordinate indices
# [1] "<val:double> [i=1:2,1000,0,j=2:3,1000,0]"

z <- translate(y)  # Reset origin with translate, see ?translate for details
schema(z)
# [1] "<val:double> [i=0:1,1000,0,j=0:1,1000,0]"
```

## The `merge` and `redimension` functions are more efficient

New versions of these functions generate better-optimized SciDB queries than
before.
