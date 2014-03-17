\name{reshape-methods}
\docType{methods}
\alias{reshape}
\alias{reshape,scidb-method}
\title{Methods for Function \code{reshape} in Package \pkg{scidb}}
\description{
Change the dimensionality of a SciDB array object.
}

\usage{
\S4method{reshape}{scidb}(data, schema, shape, dimnames, chunks, `eval`=FALSE)
}
\arguments{
  \item{data}{A \code{scidb} array object.}
  \item{schema}{Optional character-valued schema.}
  \item{shape}{An integer vector of new dimension sizes.}
  \item{dimnames}{An optional integer vector of new dimension names.}
  \item{chunks}{An optional integer vector of new dimension chunk sizes.}
  \item{eval}{(Optional) If true, execute the query and store the reult array. Otherwise defer evaluation.}
}
\details{
If \code{schema} is specified the other shape arguments are ignored.

The product of the entries of the \code{shape} vector must match the product of the dimensions of \code{data}.

The optional \code{dimnames} and \code{chunks} arguments must match the length of \code{shape} if they are supplied.
}
\value{A \code{scidb} reference object. }
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\examples{
\dontrun{
# Create a 5 x 4 matrix
X <- as.scidb(matrix(rnorm(20),5))

# Reshape into a 3-dimensional 2x2x5 array
Y <- reshape(X, c(2,2,5))
}
}
