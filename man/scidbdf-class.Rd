\name{scidbdf-class}
\Rdversion{1.1}
\docType{class}
\alias{scidbdf-class}
\alias{is.scidbdf,scidbdf-method}
\alias{is.scidbdf,ANY-method}
\alias{print,scidbdf-method}
\alias{head,scidbdf-method}
\alias{tail,scidbdf-method}
\alias{show,scidbdf-method}
\alias{aggregate,scidbdf-method}

\title{Class \code{"scidbdf"}}
\description{A class that represents 1D SciDB arrays similarly to R data.frames.}
\section{Objects from the Class}{
Objects can be created by calls of the form \code{new("scidbdf", ...)},
\code{scidb("ARRAY_NAME", ...)}, or \code{as.scidb(R_DATA_FRAME, ...)}.
}
\section{Slots}{
  \describe{
    \item{\code{call}:}{Object of class \code{"call"} How scidb was called. }
    \item{\code{name}:}{Object of class \code{"character"} scidb array name. }
    \item{\code{D}:}{Object of class \code{"list"} List of scidb dimension information. }
    \item{\code{dim}:}{Object of class \code{"numericOrNULL"} Vector of dimension lengths.}
    \item{\code{length}:}{Object of class \code{"numeric"} Length of array object. }
    \item{\code{attributes}:}{Object of class \code{"character"} Vector of all available attributes for the SciDB array. }
    \item{\code{nullable}:}{Object of class \code{"logical"} Is the attribute nullable (TRUE/FALSE)? }
    \item{\code{types}:}{Object of class \code{"character"} Vector of SciDB types for all the array attributes. }
    \item{\code{colClasses}:}{Object of class \code{"character"} Optional vector of read.table-style column classes used to parse messages from SciDB. }
    \item{\code{gc}:}{Object of class \code{"environment"} An environment used to link the SciDB array to the R garbage collector. }
    \item{\code{.S3Class}:}{Object of class \code{"character"} ~~ }
  }
}
\section{Methods}{
  \describe{
    \item{is.scidbdf}{\code{signature(x = "scidbdf")}: ... }
    \item{is.scidbdf}{\code{signature(x = "ANY")}: ... }
    \item{print}{\code{signature(x = "scidbdf")}: ... }
    \item{head}{\code{signature(x = "scidbdf")}: ... }
    \item{tail}{\code{signature(x = "scidbdf")}: ... }
    \item{aggregate}{\code{signature(x = "scidbdf")}: \code{aggregate(x, by, FUN, eval=TRUE)}

Group the scidbdf, scidb, or scidbexpr object `x` by dimensions or attributes in
the array or by another SciDB array object, applying the valid SciDB
aggregation function `FUN` expressed as a character string to the groups. Set
eval to TRUE to execute the aggregation and return a scidb object; set eval
to FALSE to return a scidbexpr object, which is essentially a character
string describing the query.

The `by` argument must be a list of dimension names and/or attribute names
in the array `x` to group by. If the list contains attribute names, they must
be of type int64.

The first element of the `by` list may optionally be a scidbdf or scidb object.
When this is the case, the SciDB array represented by the first element of the
`by` list will be joined with the SciDB array represented by the `x` argument
using the SciDB cross_join operator applied to common dimensions in the `x` and
`by` arrays. The resulting aggregation will be grouped by  the attributes in
the `by` array, which must be of type int64. Optionally, additional dimension
and/or attribute names of the joined arrays may be included in the `by` list to
group along.

\code{A = as.scidb("iris"); XXX WRITE ME XXX}

}
    \item{show}{\code{signature(object = "scidbdf")}: ... }
	 }
}
\section{Notes}{

Like the related \code{scidb} class, the \code{scidbdf} class represents
SciDB arrays as R objects. The \code{scidbdf} class presents 1-D SciDB arrays,
potentially with many SciDB attributes (variables), as a data.frame-like object.

}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\keyword{classes}
