\name{merge-methods}
\docType{methods}
\alias{merge}
\alias{merge,scidb-method}
\alias{merge,scidbdf-method}
\alias{merge,scidbexpr-method}
\title{Methods for Function \code{merge} in Package \pkg{scidb}}
\description{
SciDB \code{cross_join} and \code{join} operations.
}
\usage{
\S4method{merge}{scidb}(X,Y, by, eval)
\S4method{merge}{scidbdf}(X,Y, by, eval)
\S4method{merge}{scidbexpr}(X,Y, by, eval)
}
\arguments{
  \item{X}{A \code{scidb}, \code{scidbdf}, or \code{scidexpr} object.}
  \item{Y}{A \code{scidb}, \code{scidbdf}, or \code{scidexpr} object.}
  \item{by}{Either a single character indicating a dimension name common to both arrays to join on, or a two-element list of character vectors of array dimensions to join on.}
  \item{eval}{(Optional) If true, execute the query and return a \code{scidb} or \code{scidbdf} object. Otherwise defer evaluation and return a \code{scidbexpr} object. By default, \code{eval} is set to defer evaluation only when called from functions in the \pkg{scidb} package.}
}
\details{
XXX
}
\value{A \code{scidb}, \code{scidbdf}, or \code{scidbexpr} reference object depending on the input type and value of \code{eval}. }
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\examples{
\dontrun{
# Example
}
}
