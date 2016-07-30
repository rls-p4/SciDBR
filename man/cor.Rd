\docType{methods}
\name{cor,scidb-method}
\alias{cor,scidb-method}
\title{Correlation matrix
This function is more limited than R's default \code{cor} function. It can
only compute a correlation matrix from a data matrix without any missing value
handling.}
\usage{
\S4method{cor}{scidb}
(x, y = NULL, use = "everything", method = c("pearson", "kendall",
  "spearman"))
}
\arguments{
\item{x}{a 2-d scidb array with a single numeric attribute}

\item{y}{UNUSED, limited to correlation matrix in the SciDB case}

\item{use}{UNUSED, limited to "everything" in the SciDB case}

\item{method}{UNUSED, limited to "pearson" in the SciDB case}
}
\value{
correlation matrix of \code{x} (as a SciDB array)
}
\description{
Correlation matrix
This function is more limited than R's default \code{cor} function. It can
only compute a correlation matrix from a data matrix without any missing value
handling.
}

