\name{dist}
\docType{methods}
\alias{dist}
\alias{dist,scidb-method}
\concept{dissimilarity}
\title{Distance Matrix Computation}
\description{
  Compute and return a distance matrix that contains the Euclidean
  distance between rows of a data matrix.
}
\usage{
\S4method{dist}{scidb}(x)
}
\arguments{
  \item{x}{a numeric matrix object of class \code{scidbdf}.}
}
\details{
Unlike R's default distance matrix function, this method
only computes Euclidean distance. The \code{(i,j)} position of
the output matrix contains the Euclidean distance between rows
\code{i} and \code{j} of the data matrix \code{x}.

Missing values are not allowed.

}
\value{
  \code{dist} returns a distance matrix object of class \code{"scidb"}.
}
\seealso{
  \code{\link{hclust}}.
  \code{\link{dist}}.
}
