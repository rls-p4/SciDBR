\name{aggregate-methods}
\docType{methods}
\alias{aggregate}
\alias{aggregate,scidb-method}
\alias{aggregate,scidbdf-method}
\alias{aggregate,scidbexpr-method}
\title{Methods for Function \code{aggregate} in Package \pkg{scidb}}
\description{
Aggregate a SciDB array object grouped by a subset of its dimensions and/or attributes.
}

\usage{
\S4method{aggregate}{scidb}(x, by, FUN, eval)
\S4method{aggregate}{scidbdf}(x, by, FUN, eval)
\S4method{aggregate}{scidbexpr}(x, by, FUN, eval)
}
\arguments{
  \item{x}{A \code{scidb}, \code{scidbdf}, or \code{scidexpr} object.}
  \item{by}{Either a single character string or a list of array dimension and/or attribute names to group by; or a SciDB array reference object to group by.}
  \item{FUN}{A character string representing a SciDB aggregation expression.}
  \item{eval}{A logical value: if TRUE then evaluate the \code{bind} expression and return a \code{scidb} object, otherwise return a \code{scidbexpr} object that contains a character string representation of the un-evaluated SciDB query. By default, eval is set to FALSE if \code{bind} is called from another function in the scidb package.}
}
\details{
Group the \code{scidbexpr}, \code{scidb}, or \code{scidbexpr} array object \code{x}
by dimensions and/or attributes in the array.  applying the valid SciDB
aggregation function \code{FUN} expressed as a character string to the groups. Set
eval to TRUE to execute the aggregation and return a scidb object; set eval to
FALSE to return a scidbexpr object, which is essentially a character string
describing the query.

The \code{by} argument must be a list of dimension names and/or attribute names
in the array \code{x} to group by, or a SciDB array reference object.

The argument \code{by} may be a list of dimension names and/or attributes of the
array \code{x}. Attributes that are not of type int64 will be `factorized` first
and replaced by enumerated int64 values that indicate each unique level (this
requires SciDB 13.6 or higher).

When \code{by} is a SciDB array it must contain one or more common dimensions
with \code{x}.  The two arrays will be joined (using SciDB
\code{cross_join(x,by)} and the resulting array will be grouped by the
attributes in the \code{by} array. This is similar to the usual R data.frame
aggregate method.
}
\value{A \code{scidbdf} reference object. }
\note{
The \code{aggregate} function always produces a \code{scidbdf} object. In particular,
evaluation of \code{aggregate} cannot be deferred; this will likely be possible
after the 13.9 release of SciDB.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\examples{
\dontrun{
library("scidb")
scidbconnect()

# Create a copy of the iris data frame in a 1-d SciDB array named "iris."
# Note that SciDB attribute names will be changed to conform to SciDB
# naming convention.
x <- as.scidb(iris,name="iris")

# Compute average Petal_Length by Species:
a <- aggregate(x, by="Species", FUN="avg(Petal_Length)")

# Aggregation by an auxillary vector (which in this example comes from
# an R data frame)--also note any valid SciDB aggregation expression may
# be used:
y <- as.scidb(data.frame(sample(1:4,150,replace=TRUE)))
a <- aggregate(x, by=y, FUN="avg(Petal_Width) as apw, min(Sepal_Length) as msl")
}
}
