\name{project}
\alias{project}
\title{
project
}
\description{
The \code{project} function is a wrapper to the SciDB `project` operator.
It creates a new array that is a subset of the input array limited
to a set of specified attributes.
}
\usage{
project(X, attributes, eval = TRUE)
}
\arguments{
  \item{X}{A scidb, scidbdf, or scidbexpr object}
  \item{attributes}{A character vector of attributes to project on to}
  \item{eval}{A logical value: if TRUE then evaluate the \code{project} expression and return a \code{scidb} object, otherwise return a \code{scidbexpr} object that contains a character string representation of the un-evaluated SciDB query.}
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
x <- as.scidb(iris)
y <- project(x, c("Sepal_Length","Petal_Length","Species"))

print(head(y))
}}
