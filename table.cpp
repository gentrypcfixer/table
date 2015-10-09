#include <unistd.h>
#include <errno.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include "table.h"

#undef TABLE_DIMENSIONS_DEBUG_PRINTS

using namespace std;
//using namespace std::tr1;


namespace table {

int major_ver() { return TABLE_MAJOR_VER; }
int minor_ver() { return TABLE_MINOR_VER; }

void resize_buffer(char*& buf, char*& next, char*& end, size_t min_to_add, char** resize_end)
{
  char* old = buf;
  size_t size = next - buf;
  size_t cap = end - buf;
  size_t resize = resize_end ? end - *resize_end : 0;

  size_t new_cap = cap * 2;
  if(size + min_to_add > new_cap) new_cap = size + min_to_add;
  buf = new char[new_cap];
  memcpy(buf, old, size);
  delete[] old;

  next = buf + size;
  end = buf + new_cap;
  if(resize_end) *resize_end = end - resize;
}


void generate_substitution(const char* token, const char* replace_with, const int* ovector, int num_captured, char*& buf, char*& next, char*& end)
{
  for(const char* rp = replace_with; *rp;) {
    if(*rp == '\\' && isdigit(*(rp + 1))) {
      ++rp; long int backref = strtol(rp, const_cast<char**>(&rp), 0);
      if(backref < num_captured && ovector[backref * 2] >= 0) {
        const char* cs = token + ovector[backref * 2];
        const char* ce = token + ovector[backref * 2 + 1];
        while(cs < ce) {
          if(next >= end) resize_buffer(buf, next, end);
          *next++ = *cs++;
        }
      }
    }
    else {
      if(next >= end) resize_buffer(buf, next, end);
      *next++ = *rp++;
    }
  }
  if(next >= end) resize_buffer(buf, next, end);
  *next++ = '\0';
}

static const double pow10[] = {1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0, 1000000.0, 10000000.0, 100000000.0, 1000000000.0};

static void strreverse(char* begin, char* end)
{
    char aux;
    while (end > begin) aux = *end, *end-- = *begin, *begin++ = aux;
}

int dtostr(double value, char* str, int prec)
{
  if(isnan(value)) { str[0] = '\0'; return 0; }
  const double thres = (double)(0x7FFFFFFF);
  if(value > thres || value < -thres) { return sprintf(str, "%.6g", value); }

  // precision of >= 10 can lead to overflow errors
  if (prec < 0) { prec = 0; }
  else if (prec > 9) { prec = 9; }

  bool neg = 0;
  if(value < 0) { neg = 1; value = -value; }
  int whole = (int)value;
  double tmp = (value - whole) * pow10[prec];
  uint32_t frac = (uint32_t)(tmp);
  double diff = tmp - frac;

  if(diff > 0.5) { ++frac; if(frac >= pow10[prec]) { frac = 0; ++whole; } } // handle rollover, e.g.  case 0.99 with prec 1 is 1.0
  else if(diff == 0.5 && ((frac == 0) || (frac & 1))) { ++frac; } // if halfway, round up if odd, OR if last digit is 0.  That last part is strange

  char* wstr = str;
  if(prec == 0) {
    diff = value - whole;
    if(diff > 0.5) { ++whole; } // greater than 0.5, round up, e.g. 1.6 -> 2
    else if(diff == 0.5 && (whole & 1)) { ++whole; } // exactly 0.5 and ODD, then round up; 1.5 -> 2, but 2.5 -> 2
  }
  else if(frac) {
    int count = prec;
    while(!(frac % 10)) { --count; frac /= 10; }
    do { --count; *wstr++ = '0' + char(frac % 10); } while(frac /= 10);
    while(count-- > 0) *wstr++ = '0';
    *wstr++ = '.';
  }

  do { *wstr++ = '0' + char(whole % 10); } while(whole /= 10);
  if(neg) *wstr++ = '-';
  *wstr = '\0';
  strreverse(str, wstr-1);

  return wstr - str;
}

static float gammln(float xx)
{
  const double cof[6] = {
    76.18009172947146, -86.50532032941677,
    24.01409824083091, -1.231739572450155,
    0.1208650973866179e-2, -0.5395239384953e-5
  };
  double y = xx, x = xx;
  double tmp = x + 5.5;
  tmp -= (x + 0.5) * log(tmp);
  double ser = 1.000000000190015;
  for(int j = 0; j <= 5; ++j) ser += cof[j] / ++y;
  return -tmp + log(2.5066282746310005 * ser / x);
}

static float betacf(float a, float b, float x)
{
  const int MAXIT = 100;
  const float EPS = 3.0e-7;
  const float FPMIN = 1.0e-30;

  float qab = a + b;
  float qap = a + 1.0f;
  float qam = a - 1.0f;
  float c = 1.0f;
  float d = 1.0f - qab * x / qap;
  if(fabs(d) < FPMIN) d = FPMIN;
  d = 1.0f / d;
  float h = d;
  int m = 1;
  for(; m <= MAXIT; m++) {
    int m2 = 2 * m;
    float aa = m * (b - m) * x / ((qam + m2) * (a + m2));
    d = 1.0f + aa * d;
    if(fabs(d) < FPMIN) d = FPMIN;
    c = 1.0f + aa / c;
    if(fabs(c) < FPMIN) c = FPMIN;
    d = 1.0f / d;
    h *= d * c;
    aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2));
    d = 1.0f + aa * d;
    if(fabs(d) < FPMIN) d = FPMIN;
    c = 1.0f + aa / c;
    if(fabs(c) < FPMIN) c = FPMIN;
    d = 1.0f / d;
    float del = d * c;
    h *= del;
    if(fabs(del - 1.0f) < EPS) break;
  }
  if(m > MAXIT) throw runtime_error("a or b too big, or MAXIT too small in betacf");
  return h;
}

float ibeta(float a, float b, float x)
{
  if(x < 0.0f || x > 1.0f) throw runtime_error("Bad x in routine betai");

  float bt = 0.0f;
  if(x != 0.0f && x != 1.0f)
    bt = exp(gammln(a + b) - gammln(a) - gammln(b) + a * log(x) + b * log(1.0 - x));

  if(x < (a + 1.0f) / (a + b + 2.0f)) return bt * betacf(a, b, x) / a;
  else return 1.0f - bt * betacf(b, a, 1.0f - x) / b;
}


}
