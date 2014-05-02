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
#include <string.h>
#ifdef WIN32
#include <windows.h>
#else
#include <sys/mman.h>
#endif

#include <R.h>
#define USE_RINTERNALS
#include <Rinternals.h>
#include <Rdefines.h>


/* Convenience function that compares two character strings.
 * Returns zero if not matching, 1 if matching.
 */
int scmp (const char *a, const char *b)
{
  int ans = strncmp(a, b, strlen(b))==0;
  return ans;
}

/* Create an R vector of length 'len' and of a type that maps
 * to one of the supported SciDB types supplied as a string name.
 * The mapping is hard-coded right now, probably should make this 
 * more flexible/programmable.
 * SciDB type    R type
 * ----------    ---------
 * bool          logical
 * char          character
 * datetime      double (aka real, numeric)
 * datetimetz    double
 * float         double
 * double        double
 * int64         double
 * uint64        double
 * uint32        double
 * int8          integer
 * uint8         integer
 * int16         integer
 * uint16        integer
 * int32         integer
 * string        string
 *
 * Other types are not supported and will throw an error.
 */
SEXP scidb_type_vector (const char *type, int len)
{
  SEXP ans;
  if(scmp(type,"bool"))
  {
    ans = NEW_LOGICAL(len);
    return ans;
  } else if(scmp(type,"char"))
  {
    ans = NEW_CHARACTER(len);
    return ans;
  } else if(scmp(type,"datetime") || scmp(type,"datetimetz") || scmp(type,"float") ||
            scmp(type,"double") || scmp(type,"int64") || scmp(type,"uint64") || scmp(type,"unit32"))
  {
    ans = NEW_NUMERIC(len);
    return ans;
  } else if(scmp(type,"int8") || scmp(type,"uint8") || scmp(type,"int16") || scmp(type,"uint16") ||
            scmp(type,"int32"))
  {
    ans = NEW_INTEGER(len);
    return ans;
  } else if(scmp(type,"string"))
  {
    ans = NEW_STRING(len);
    return ans;
  }
  error("Unsupported type");
  return R_NilValue;
}

/* Extract a single value from the binary encoded SciDB data pointer p.
 * The pointer's address is updated.
 * type is a string that corresponds to the SciDB type (see above)
 * nullable is an integer, 0 meaning not nullable 1 nullable.
 * vec is the output vector of appropriate R type from scidb_type_vector
 * i is the position in vec to place the converted value
 */
void scidb_value (char **p, const char *type, int nullable, SEXP vec, int i)
{
//printf("scidb_value ");
  int isnull = 0;
  if(nullable)
  {
    isnull = (int)(char)*((char *)*p);
    isnull = isnull<127;
    (*p)++;
  }
//printf("type %s ",type);
//printf("isnull %d ",isnull);
  if(scmp(type,"int64"))
  {
    long long ll = (long long)*((long long *)*p);
    (*p)+=8;
//printf("val %lld\n",ll);
    if(isnull)
    {
      REAL(vec)[i] = NA_REAL;
      return;
    }
// XXX CHECK BOUNDS HERE XXX
    REAL(vec)[i] = (double)ll;
    return;
  } else if(scmp(type,"double"))
  {
    double d = (double)*((double *)*p);
    (*p)+=8;
//printf("val %f\n",d);
    if(isnull)
    {
      REAL(vec)[i] = NA_REAL;
      return;
    }
    REAL(vec)[i] = d;
    return;
  } else if(scmp(type,"string"))
  {
    unsigned int len = (unsigned int)*((unsigned int*)*p);
    (*p)+=4;
//printf("len %d ",len);
    if(isnull)
    {
      (*p)+=len;
      SET_STRING_ELT(vec,i,NA_STRING);
      return;
    }
// XXX bounds checks ? how long can a string be in SciDB?
    char *buf = (char *)calloc(len,1);
    memcpy(buf, *p, len);
    (*p)+=len;
//printf("%s\n",buf);
    SET_STRING_ELT(vec,i,mkChar(buf));
    free(buf);
    return;
  }
  error("Unsupported type: ",type);
}

/*
 * Convert a raw binary unpacked SciDB array to a list.
 * M: Number of rows  (int) to try to unpack
 * TYPES: Character vector of SciDB types, of length N
 * NULLABLE: Logical vector of SciDB nullability, of length N
 * DATA: R RAW vector with the binary SciDB data
 * OFFSET: Offset byte to start reading from (REAL)
 *
 * Output: An n+2-element list:
 * Elements 1,2,...,n are the parsed data vectors
 * Element n is the number of rows retrieved <= M
 * Element n+1 is the final byte offset into DATA
 */
SEXP scidb_parse (SEXP M, SEXP TYPES, SEXP NULLABLE, SEXP DATA, SEXP OFFSET)
{
  int nullable, i=0,j;
  SEXP col, val, ans;
  int m = INTEGER(M)[0];
  R_xlen_t n = XLENGTH(TYPES);
  double doffset = REAL(OFFSET)[0];
  size_t offset = (size_t)doffset;
  R_xlen_t s = XLENGTH(DATA);
  char *p = (char *)RAW(DATA);
  char *q = p;
  p+=offset;

// Check length mismatch
  if(n!=XLENGTH(NULLABLE)) error("length(TYPES) must match length(NULLABLE)");
// create the data frame list
  ans = PROTECT (NEW_LIST (n+2));
  int protectCount = 1;

// fill in the list with columns of an appropriate type and size
  for (j = 0; j < n; ++j)
  {
    SET_VECTOR_ELT(ans,j, PROTECT(scidb_type_vector(CHAR(STRING_ELT(TYPES,j)), m)));
    protectCount++;
  }
// Make sure starting condition is valid
  if(p-q >= s) goto end;
  for(i=0;i<m;++i)
  {
    for(j=0;j<n;++j)
    {
      col = VECTOR_ELT(ans,j);
// XXX Add max bytes allowed to read here...
      scidb_value(&p, CHAR(STRING_ELT(TYPES,j)), INTEGER(NULLABLE)[j], col, i);
      if(p-q >= s)
      {
        i++;
        goto end;
      }
    }
  }

end:
  SET_VECTOR_ELT(ans, n, ScalarInteger(i));
  SET_VECTOR_ELT(ans, n+1, ScalarReal((double)(p-q)));
  UNPROTECT (protectCount);
  return (ans);
}
