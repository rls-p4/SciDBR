\name{scidbexpr-class}
\Rdversion{1.1}
\docType{class}
\alias{scidbexpr-class}
\alias{aggregate,scidbexpr-method}

\title{Class \code{"scidbexpr"}}
\description{A class that represents a SciDB query expression. This class is intended to distinguish SciDB expressions from general character strings.}
\section{Methods}{
  \describe{
    \item{aggregate}{\code{signature(x = "scidbexpr")}: \code{aggregate(x, by, FUN, eval=TRUE)}

Group the scidbexpr, scidb, or scidbexpr object `x` by dimensions or attributes in
the array or by another SciDB array object, applying the valid SciDB
aggregation function `FUN` expressed as a character string to the groups. Set
eval to TRUE to execute the aggregation and return a scidb object; set eval
to FALSE to return a scidbexpr object, which is essentially a character
string describing the query.

The `by` argument must be a list of dimension names and/or attribute names
in the array `x` to group by. If the list contains attribute names, they must
be of type int64.

The first element of the `by` list may optionally be a scidbexpr or scidb object.
When this is the case, the SciDB array represented by the first element of the
`by` list will be joined with the SciDB array represented by the `x` argument
using the SciDB cross_join operator applied to common dimensions in the `x` and
`by` arrays. The resulting aggregation will be grouped by  the attributes in
the `by` array, which must be of type int64. Optionally, additional dimension
and/or attribute names of the joined arrays may be included in the `by` list to
group along.

\code{A = as.scidb("iris"); XXX WRITE ME XXX}

}
    \item{show}{\code{signature(object = "scidbexpr")}: ... }
	 }
}
\section{Notes}{
This class is intended to distinguish a SciDB query expression from a general
character string. Objects of this class work with many functions that also
work with scidb and scidbdf objects.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\keyword{classes}
