/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2013 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <math.h>
#include <signal.h>
#ifndef WIN32
#include <sys/mman.h>
#endif
#include <string.h>

#include <R.h>
#define USE_RINTERNALS
#include <Rinternals.h>

#define LINESIZE 4096

typedef struct
{
  char *s;
  size_t len;
  size_t pos;
} abuf;

abuf
newbuf(size_t size)
{
  abuf a;
  if(size < 1) error("Invalid buffer size");
  a.s   = (char *)malloc(size);
  if(!a.s) error("Not enough memory");
  a.len = size;
  a.pos = 0;
  return a;
}

void
append(abuf *buf, char *string)
{
  char *new;
  int l = strlen(string) + 1;
  if(buf->len - buf->pos < l)
  {
    if(buf->len + l > buf->len*2)
      buf->len = buf->len + l;
    else
      buf->len = buf->len*2;
    new     = realloc(buf->s, buf->len);
    if(new) buf->s = new;
    else
    {
      if(buf->s) free(buf->s);
      error ("Not enough memory.");
    }
  }
  strncpy(&buf->s[buf->pos], string, l); // Includes terminating \0
  buf->pos+= l-1; // Does not include terminating \0
}

/* df2scidb converts a data.frame object to SciDB ASCII input format, returning
 * the result in a character string.  It only handles double, int, logical and
 * string data types in the data.frame.  chunk is the integer SciDB 1-D array
 * chunk size.  start is the integer SciDB 1-D array starting index.
 */
SEXP
df2scidb (SEXP A, SEXP chunk, SEXP start, SEXP REALFORMAT)
{
  int j, k, m, n, m1, m2, J, M, logi;
  char line[LINESIZE];
  abuf buf;
  double x;
  SEXP ans;
  const char *rfmt = CHAR(STRING_ELT(REALFORMAT,0));
  double S = *(REAL (start));
  int R = *(INTEGER (chunk));

  buf = newbuf(1048576); // 1MB initial buffer

  n = length (A);
  m = nrows (VECTOR_ELT (A, 0));
  M = ceil (((double) m) / R);

  for (J = 0; J < M; ++J)
    {
      m1 = J * R;
      m2 = (J + 1) * R;
      if (m2 > m)
        m2 = m;
      snprintf(line, LINESIZE, "{%ld}[\n", (long) (m1 + S));
      append(&buf,line);
      for (j = m1; j < m2; ++j)
        {
          append(&buf, "(");
          for (k = 0; k < n; ++k)
            {
              memset(line,0,LINESIZE);
// Check for factor and print factor level instead of integer
              if (!
                  (getAttrib (VECTOR_ELT (A, k), R_LevelsSymbol) ==
                   R_NilValue))
                {
                  if ((INTEGER (VECTOR_ELT (A, k))[j]) != R_NaInt)
                  {
                    const char *vi =
                      translateChar (STRING_ELT
                                   (getAttrib
                                    (VECTOR_ELT (A, k), R_LevelsSymbol),
                                    INTEGER (VECTOR_ELT (A, k))[j] - 1));
                    snprintf(line, LINESIZE, "\"%s\"", vi);
                    append(&buf,line);
                  }
                }
              else
                {
                  switch (TYPEOF (VECTOR_ELT (A, k)))
                    {
                    case LGLSXP:
                      logi = 0;
                      if ((LOGICAL (VECTOR_ELT (A, k))[j]) != NA_LOGICAL)
                        logi = (int) LOGICAL (VECTOR_ELT (A, k))[j];
                      if (logi)
                        snprintf(line, LINESIZE, "%s", "true");
                      else
                        snprintf(line, LINESIZE, "%s", "false");
                      append(&buf,line);
                      break;
                    case INTSXP:
                      if ((INTEGER (VECTOR_ELT (A, k))[j]) != R_NaInt)
                        snprintf(line, LINESIZE, "%d", INTEGER (VECTOR_ELT (A, k))[j]);
                      append(&buf,line);
                      break;
                    case REALSXP:
                      x = REAL (VECTOR_ELT (A, k))[j];
                      if (!ISNA (x))
                        snprintf(line, LINESIZE, rfmt, x);
                      append(&buf,line);
                      break;
                    case STRSXP:
                      if (STRING_ELT (VECTOR_ELT (A, k), j) != NA_STRING)
                        snprintf(line, LINESIZE, "\"%s\"",
                                 CHAR (STRING_ELT (VECTOR_ELT (A, k), j)));
                      append(&buf,line);
                      break;
                    default:
                      break;
                    }
                }
              if (k == n - 1)
                append(&buf, ")");
              else
                append(&buf, ",");
            }
          if (j < m2 - 1)
            append(&buf, ",");
          else
            append(&buf, "];");
        }
    }

// XXX possible R bug here in mkString? Sometimes extra chars appended
// (beyond length defined above...)
  ans = mkString(buf.s);
  if(buf.s) free(buf.s);
  return ans;
}


/* Read SciDB single-attribute binary output from the specified
 * file.
 * TYPE must be an SEXP of the correct output type.
 * DIM is a vector of maximum output dimension lengths (INTEGER)
 * NULLABLE is an integer, 1 indicates we need to parse for nullable
 * output from SciDB.
 *
 * It expects the binary input file to include integer dimensions
 * like:
 * value, int64, int64, ..., value, int64, int64, ..., etc.
 * It returns a list of two elements, each of length NR*NC:
 * 1. A vector of values  of length L = prod(DIM)
 * 2. A double precision matrix of L rows and number of dimension columns
 *    The matrix contains the dimension indices of each value.
 * Only values with corresponding row/column indices that are not marked
 * NA are valid.
 */
SEXP
scidb2m (SEXP file, SEXP DIM, SEXP TYPE, SEXP NULLABLE)
{
  int j, k, l;
  long long i;
  size_t m;
  FILE *fp;
  SEXP A, ans, I;
  double x;
  char xc[2];
  int xi;
  char a;
  char nx;
  int nullable = INTEGER(NULLABLE)[0];
  const char *f = CHAR (STRING_ELT (file, 0));
  fp = fopen (f, "r");
  if (!fp)
    error ("Invalid file");

  l = 1;
  for(j=0;j<length(DIM);++j) l = l*INTEGER(DIM)[j];

/* We reserve the maximum elements and fill the **indices** with NA. The
 * variable A contains the data and I the indices. Remember, R does not have
 * 64-bit integers, so we use doubles instead to store indices.
 */
  PROTECT (A = allocVector (TYPEOF (TYPE), l));
  PROTECT (I = allocMatrix (REALSXP, l, length(DIM)));
  for(j=0;j<l*length(DIM);++j) REAL(I)[j] = NA_REAL;
  nx = 1;

  switch (TYPEOF (TYPE))
    {
    case REALSXP:
      for (j = 0; j < l; ++j)
        {
          REAL(A)[j] = NA_REAL;
          if(nullable) {
            m = fread(&nx, sizeof(char), 1, fp);
            if (m < 1) break;
          }
          m = fread (&x, sizeof (double), 1, fp);
          if (m < 1) break;
          for(k=0;k<length(DIM);++k) {
            m = fread(&i, sizeof(long long),1,fp);
            if (m < 1) break;
            REAL(I)[j + k*l] = (double)i;
          }
          if((int)nx != 0) REAL (A)[j] = x;
        }
      break;
    case STRSXP:
      for (j = 0; j < l; ++j)
        {
          SET_STRING_ELT (A, j, NA_STRING);
          if(nullable) {
            m = fread(&nx, sizeof(char), 1, fp);
            if (m < 1) break;
          }
          memset(xc,0,2);
          m = fread (xc, sizeof (char), 1, fp);
          if (m < 1) break;
          for(k=0;k<length(DIM);++k) {
            m = fread(&i, sizeof(long long),1,fp);
            if (m < 1) break;
            REAL(I)[j + k*l] = (double)i;
          }
          if((int)nx != 0) SET_STRING_ELT (A, j, mkChar (xc));
        }
      break;
    case LGLSXP:
      for (j = 0; j < l; ++j)
        {
          LOGICAL (A)[j] = NA_LOGICAL;
          if(nullable) {
            m = fread(&nx, sizeof(char), 1, fp);
            if (m < 1) break;
          }
          m = fread (&a, sizeof (char), 1, fp);
          if (m < 1) break;
          for(k=0;k<length(DIM);++k) {
            m = fread(&i, sizeof(long long),1,fp);
            if (m < 1) break;
            REAL(I)[j + k*l] = (double)i;
          }
          if((int)nx != 0) LOGICAL (A)[j] = (int)a;
        }
      break;
    case INTSXP:
      for (j = 0; j < l; ++j)
        {
          INTEGER (A)[j] = R_NaInt;
          if(nullable) {
            m = fread(&nx, sizeof(char), 1, fp);
            if (m < 1) break;
          }
          m = fread (&xi, sizeof (int), 1, fp);
          if (m < 1) break;
          for(k=0;k<length(DIM);++k) {
            m = fread(&i, sizeof(long long),1,fp);
            if (m < 1) break;
            REAL(I)[j + k*l] = (double)i;
          }
          if((int) nx != 0) INTEGER (A)[j] = xi;
        }
      break;
    default:
      break;
    };
  ans = PROTECT(allocVector(VECSXP, 2));
  SET_VECTOR_ELT(ans, 0, A);
  SET_VECTOR_ELT(ans, 1, I);

  fclose (fp);
  UNPROTECT (3);
  return ans;
}

/* Parse SciDB unpack results of a single-attribute array for a limited set
 * of types.
 *
 * DATA: An R RAW vector
 * NDIM: The number of array dimensions (INTEGER)
 * LEN: The number of cells (INTEGER)
 * TYPE: The data type
 * NULLABLE: Is the data SciDB-NULLABLE (INTEGER)?
 *
 * Returns a two-element list:
 * 1. A length-LEN vector of cell values (TYPE)
 * 2. A LEN x NDIM matrix of cell coordinate (DOUBLE)
 *
 */
SEXP
scidbparse (SEXP DATA, SEXP NDIM, SEXP LEN, SEXP TYPE, SEXP NULLABLE)
{
  int j, k, l, ndim;
  long long i;
  SEXP A, I, ans;
  double x;
  char xc[2];
  int xi;
  char a;
  char nx;
  int nullable = INTEGER(NULLABLE)[0];
  char *raw = (char *)RAW(DATA);

  l = (long long)INTEGER(LEN)[0];
  ndim = INTEGER(NDIM)[0];

  PROTECT (A = allocVector (TYPEOF (TYPE), l));
  PROTECT (I = allocMatrix (REALSXP, l, ndim));
  nx = 1;

  switch (TYPEOF (TYPE))
    {
    case REALSXP:
      for (j = 0; j < l; ++j)
        {
          for(k=0;k<ndim;++k) {
            memcpy(&i, raw, sizeof(long long));  raw+=sizeof(long long);
            REAL(I)[j + k*l] = (double)i;
          }
          REAL(A)[j] = NA_REAL;
          if(nullable) {
            memcpy(&nx, raw, sizeof(char));  raw++;
          }
          memcpy(&x, raw, sizeof(double)); raw+=sizeof(double);
          if((int)nx != 0) REAL (A)[j] = x;
        }
      break;
    case STRSXP:
      for (j = 0; j < l; ++j)
        {
          for(k=0;k<ndim;++k) {
            memcpy(&i, raw, sizeof(long long));  raw+=sizeof(long long);
            REAL(I)[j + k*l] = (double)i;
          }
          SET_STRING_ELT (A, j, NA_STRING);
          if(nullable) {
            memcpy(&nx, raw, sizeof(char));  raw++;
          }
          memset(xc,0,2);
          memcpy(&xc, raw, sizeof(char)); raw+=sizeof(char);
          if((int)nx != 0) SET_STRING_ELT (A, j, mkChar (xc));
        }
      break;
    case LGLSXP:
      for (j = 0; j < l; ++j)
        {
          for(k=0;k<ndim;++k) {
            memcpy(&i, raw, sizeof(long long));  raw+=sizeof(long long);
            REAL(I)[j + k*l] = (double)i;
          }
          LOGICAL (A)[j] = NA_LOGICAL;
          if(nullable) {
            memcpy(&nx, raw, sizeof(char));  raw++;
          }
          memcpy(&a, raw, sizeof(char)); raw+=sizeof(char);
          if((int)nx != 0) LOGICAL (A)[j] = (int)a;
        }
      break;
    case INTSXP:
      for (j = 0; j < l; ++j)
        {
          for(k=0;k<ndim;++k) {
            memcpy(&i, raw, sizeof(long long));  raw+=sizeof(long long);
            REAL(I)[j + k*l] = (double)i;
          }
          INTEGER (A)[j] = R_NaInt;
          if(nullable) {
            memcpy(&nx, raw, sizeof(char));  raw++;
          }
          memcpy(&xi, raw, sizeof(int)); raw+=sizeof(int);
          if((int) nx != 0) INTEGER (A)[j] = xi;
        }
      break;
    default:
      break;
    };
  ans = PROTECT(allocVector(VECSXP, 2));
  SET_VECTOR_ELT(ans, 0, A);
  SET_VECTOR_ELT(ans, 1, I);
  UNPROTECT (3);
  return ans;
}
