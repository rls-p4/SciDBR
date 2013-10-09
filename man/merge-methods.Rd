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
  \item{by}{Either a single character indicating a dimension name common to both arrays to join on, or a two-element list of character vectors of array attributes dimensions to join on.}
  \item{eval}{(Optional) If true, execute the query and return a \code{scidb} or \code{scidbdf} object. Otherwise defer evaluation and return a \code{scidbexpr} object. By default, \code{eval} is set to defer evaluation only when called from functions in the \pkg{scidb} package.}
}
\details{
Certain limitaions apply for now. In particular, when \code{by} lists attributes from each source array,
then \code{by} must consist of precisely two attributes (one from each array), and is limited to an
inner join. Future versions of the \code{scidb} R package will generalize this.
}
\value{A \code{scidb}, \code{scidbdf}, or \code{scidbexpr} reference object depending on the input type and value of \code{eval}. }
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\examples{
\dontrun{
# Example
library("scidb")
scidbconnect()

# Create a copy of the iris data frame in a 1-d SciDB array named "iris."
# Note that SciDB attribute names will be changed to conform to SciDB
# naming convention.
x <- as.scidb(iris,name="iris")

a <- project(x, "Species", eval=FALSE)
b <- project(x, "Petal_Length", eval=FALSE)

c <- merge(a, b, by="row")
}
}
