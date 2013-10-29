\name{redimension}
\alias{redimension}
\title{
redimension
}
\description{
The \code{redimension} function is a wrapper to the SciDB `redimension` operator.
}
\usage{
redimension = function(x,s,eval)
}
\arguments{
  \item{x}{A SciDB array object of class scidb, scidbdf, or scidbexpr.}
  \item{s}{A SciDB array object of class scidb, scidbdf, or scidbexpr, or a character string representation of the output array schema.}
  \item{eval}{A logical value: if TRUE then evaluate the \code{redimension} expression and return a \code{scidb} object, otherwise return a \code{scidbexpr} object that contains a character string representation of the un-evaluated SciDB query. By default, eval is set to FALSE if \code{redimension} is called from another function in the scidb package.}
}
\value{
Either a \code{scidb} object or a \code{scidbexpr} object depending on the value of \code{eval}.
}
\note{
Use \code{eval=FALSE} to defer expression evaluation for efficiency. Many functions in the SciDB package accept un-evaluated expresions represented as \code{scidbexpr} objects. One can compose these objects to form complex query expressions that can be evaluated in one transaction.
}
\author{
B. W. Lewis<blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
}}
