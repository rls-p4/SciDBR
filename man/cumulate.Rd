\name{cumulate}
\alias{cumulate}
\title{
cumulate
}
\description{
Use \code{cumulate} function to compute running operations along data,
for example cumulative sums.
}
\usage{
cumulate(x, expression, dimension, eval)
}
\arguments{
  \item{x}{A scidb, scidbdf, or scidbexpr object}
  \item{expression}{Any valid SciDB aggregate expression, expressed as a character string}
  \item{dimension}{An optional array dimension name (character) to run along. The default is to use the first dimension.}
  \item{eval}{A logical value: if TRUE then evaluate the \code{cumulate} expression and return a \code{scidb} object, otherwise return a \code{scidbexpr} object that contains a character string representation of the un-evaluated SciDB query. By default, eval is set to FALSE if \code{cumulate} is called from another function in the scidb package.}
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
  \code{\link{aggregate}}
}
\examples{
\dontrun{
x <- as.scidb(iris)
y <- cumulate(x, "sum(Petal_Width)")

print(head(y))
}}
