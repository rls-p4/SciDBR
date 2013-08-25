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
\S4method{aggregate}{scidb}(x, by, FUN)
\S4method{aggregate}{scidbdf}(x, by, FUN)
\S4method{aggregate}{scidbexpr}(x, by, FUN)
}
\arguments{
  \item{x}{A \code{scidb}, \code{scidbdf}, or \code{scidexpr} object.}
  \item{by}{Either a single character string or a list of array dimension and/or attribute names to group by; or a SciDB array reference object to group by.}
  \item{FUN}{A character string representing a SciDB aggregation expression.}
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

When \code{by} is a SciDB array it must contain one or more common dimensions with \code{x}.
The two arrays will be joined (using SciDB \code{cross_join(x,by)} and the resulting array
will be grouped by the attributes in the \code{by} array. This is similar to the usual
R data.frame aggregate method.
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
# Example
}
}
