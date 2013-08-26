\name{sort-methods}
\docType{methods}
\alias{sort}
\alias{sort,scidb-method}
\alias{sort,scidbdf-method}
\alias{sort,scidbexpr-method}
\title{Methods for Function \code{sort} in Package \pkg{scidb}}
\description{
Aggregate a SciDB array object grouped by a subset of its dimensions and/or attributes.
}

\usage{
\S4method{sort}{scidb}(x, decreasing=FALSE, attributes, eval)
\S4method{sort}{scidbdf}(x, decreasing=FALSE, attributes, eval)
\S4method{sort}{scidbexpr}(x, decreasing=FALSE, attributes, eval)
}
\arguments{
  \item{x}{A \code{scidb}, \code{scidbdf}, or \code{scidexpr} object.}
  \item{decreasing}{A logical value: TRUE means sort in decreasing order.}
  \item{attributes}{A character vector of attribute names from array \code{x} to sort on}
  \item{eval}{(Optional) If true, execute the query and return a \code{scidb} or \code{scidbdf} object. Otherwise defer evaluation and return a \code{scidbexpr} object. By default, \code{eval} is set to defer evaluation only when called from functions in the \pkg{scidb} package.}
}
\details{
Create a new array reference object that represents the sorted version of array
\code{x}.  The output array reference has the same shape and number of
attributes as \code{x} but the specified attributes appear in sorted order.
}
\value{A \code{scidb},\code{scidbdf}, or \code{scidbexpr} reference object, depending on the value of \code{eval}.}
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

# Sort x by Petal_Width and Petal_Length:
a <- sort(x, attributes=c("Petal_Width","Petal_Length"))
}
}
