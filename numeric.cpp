#include <stdexcept>
#include <limits>
#include <math.h>
#include <stdio.h>
#include "table.h"

using namespace std;


namespace table {

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

static float ibeta(float a, float b, float x)
{
  if(x < 0.0f || x > 1.0f) throw runtime_error("Bad x in routine betai");

  float bt = 0.0f;
  if(x != 0.0f && x != 1.0f)
    bt = exp(gammln(a + b) - gammln(a) - gammln(b) + a * log(x) + b * log(1.0 - x));

  if(x < (a + 1.0f) / (a + b + 2.0f)) return bt * betacf(a, b, x) / a;
  else return 1.0f - bt * betacf(b, a, 1.0f - x) / b;
}


////////////////////////////////////////////////////////////////////////////////////////////////
// summarizer
////////////////////////////////////////////////////////////////////////////////////////////////

summarizer_data_t::summarizer_data_t() :
  missing(0),
  count(0),
  sum(0.0),
  sum_of_squares(0.0),
  min(numeric_limits<double>::infinity()),
  max(-numeric_limits<double>::infinity())
{}

summarizer::summarizer() : values(0), group_tokens(0) { init(); }
summarizer::summarizer(pass& out) : values(0), group_tokens(0) { init(out); }

summarizer& summarizer::init()
{
  this->out = 0;
  for(vector<pcre*>::iterator gri = group_regexes.begin(); gri != group_regexes.end(); ++gri) pcre_free(*gri);
  group_regexes.clear();
  for(vector<pair<pcre*, uint32_t> >::iterator dri = data_regexes.begin(); dri != data_regexes.end(); ++dri) pcre_free((*dri).first);
  data_regexes.clear();
  for(vector<pcre*>::iterator ei = exception_regexes.begin(); ei != exception_regexes.end(); ++ei) pcre_free(*ei);
  exception_regexes.clear();
  first_line = 1;
  column_flags.clear();
  num_data_columns = 0;
  delete[] values; values = 0;
  delete[] group_tokens; group_tokens = new char[2048];
  group_tokens_next = group_tokens;
  group_tokens_end = group_tokens + 2048;
  for(vector<char*>::iterator i = group_storage.begin(); i != group_storage.end(); ++i) delete[] *i;
  group_storage.clear();
  group_storage_next = 0;
  for(vector<summarizer_data_t*>::iterator i = data_storage.begin(); i != data_storage.end(); ++i) delete[] *i;
  data_storage.clear();
  data_storage_next = 0;
  data.clear();

  return *this;
}

summarizer& summarizer::set_out(pass& out) { this->out = &out; return *this; }
summarizer& summarizer::init(pass& out) { init(); return set_out(out); }

summarizer::~summarizer()
{
  for(vector<pcre*>::iterator gri = group_regexes.begin(); gri != group_regexes.end(); ++gri) pcre_free(*gri);
  for(vector<pair<pcre*, uint32_t> >::iterator dri = data_regexes.begin(); dri != data_regexes.end(); ++dri) pcre_free((*dri).first);
  for(vector<pcre*>::iterator ei = exception_regexes.begin(); ei != exception_regexes.end(); ++ei) pcre_free(*ei);
  delete[] values;
  delete[] group_tokens;
  for(vector<char*>::iterator i = group_storage.begin(); i != group_storage.end(); ++i) delete[] *i;
  for(vector<summarizer_data_t*>::iterator i = data_storage.begin(); i != data_storage.end(); ++i) delete[] *i;
}

summarizer& summarizer::add_group(const char* regex)
{
  const char* err; int err_off; pcre* p = pcre_compile(regex, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("summarizer can't compile group regex");
  group_regexes.push_back(p);
  return *this;
}

summarizer& summarizer::add_data(const char* regex, uint32_t flags)
{
  const char* err; int err_off; pcre* p = pcre_compile(regex, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("summarizer can't compile data regex");
  data_regexes.push_back(pair<pcre*, uint32_t>(p, flags & 0xFFFFFFFE));
  return *this;
}

summarizer& summarizer::add_exception(const char* regex)
{
  const char* err; int err_off; pcre* p = pcre_compile(regex, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("summarizer can't compile exception regex");
  exception_regexes.push_back(p);
  return *this;
}

void summarizer::process_token(const char* token)
{
  if(first_line) {
    if(!out) throw runtime_error("summarizer has no out");

    size_t len = strlen(token);
    uint32_t flags = 0;
    for(vector<pcre*>::iterator gri = group_regexes.begin(); gri != group_regexes.end(); ++gri) {
      int ovector[30]; int rc = pcre_exec(*gri, 0, token, len, 0, 0, ovector, 30);
      if(rc >= 0) { out->process_token(token); flags = 1; break; }
      else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("summarizer match error");
    }
    if(!flags) {
      for(vector<pair<pcre*, uint32_t> >::iterator dri = data_regexes.begin(); dri != data_regexes.end(); ++dri) {
        int ovector[30]; int rc = pcre_exec((*dri).first, 0, token, len, 0, 0, ovector, 30);
        if(rc >= 0) { flags |= (*dri).second; }
        else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("summarizer match error");
      }
      for(vector<pcre*>::iterator ei = exception_regexes.begin(); ei != exception_regexes.end(); ++ei) {
        int ovector[30]; int rc = pcre_exec(*ei, 0, token, len, 0, 0, ovector, 30);
        if(rc >= 0) { flags = 0; break; }
        else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("summarizer match error");
      }
    }

    if(flags & SUM_MISSING) {
      if(group_tokens_next + len + 9 >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end);
      memcpy(group_tokens_next, "MISSING(", 8); group_tokens_next += 8;
      memcpy(group_tokens_next, token, len); group_tokens_next += len;
      *group_tokens_next++ = ')'; *group_tokens_next++ = '\0';
    }
    if(flags & SUM_COUNT) {
      if(group_tokens_next + len + 7 >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end);
      memcpy(group_tokens_next, "COUNT(", 6); group_tokens_next += 6;
      memcpy(group_tokens_next, token, len); group_tokens_next += len;
      *group_tokens_next++ = ')'; *group_tokens_next++ = '\0';
    }
    if(flags & SUM_SUM) {
      if(group_tokens_next + len + 5 >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end);
      memcpy(group_tokens_next, "SUM(", 4); group_tokens_next += 4;
      memcpy(group_tokens_next, token, len); group_tokens_next += len;
      *group_tokens_next++ = ')'; *group_tokens_next++ = '\0';
    }
    if(flags & SUM_MIN) {
      if(group_tokens_next + len + 5 >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end);
      memcpy(group_tokens_next, "MIN(", 4); group_tokens_next += 4;
      memcpy(group_tokens_next, token, len); group_tokens_next += len;
      *group_tokens_next++ = ')'; *group_tokens_next++ = '\0';
    }
    if(flags & SUM_MAX) {
      if(group_tokens_next + len + 5 >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end);
      memcpy(group_tokens_next, "MAX(", 4); group_tokens_next += 4;
      memcpy(group_tokens_next, token, len); group_tokens_next += len;
      *group_tokens_next++ = ')'; *group_tokens_next++ = '\0';
    }
    if(flags & SUM_AVG) {
      if(group_tokens_next + len + 5 >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end);
      memcpy(group_tokens_next, "AVG(", 4); group_tokens_next += 4;
      memcpy(group_tokens_next, token, len); group_tokens_next += len;
      *group_tokens_next++ = ')'; *group_tokens_next++ = '\0';
    }
    if(flags & SUM_VARIANCE) {
      if(group_tokens_next + len + 10 >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end);
      memcpy(group_tokens_next, "VARIANCE(", 9); group_tokens_next += 9;
      memcpy(group_tokens_next, token, len); group_tokens_next += len;
      *group_tokens_next++ = ')'; *group_tokens_next++ = '\0';
    }
    if(flags & SUM_STD_DEV) {
      if(group_tokens_next + len + 9 >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end);
      memcpy(group_tokens_next, "STD_DEV(", 8); group_tokens_next += 8;
      memcpy(group_tokens_next, token, len); group_tokens_next += len;
      *group_tokens_next++ = ')'; *group_tokens_next++ = '\0';
    }

    column_flags.push_back(flags);
    if(flags & 0xFFFFFFFE) ++num_data_columns;
  }
  else {
    const uint32_t& flags = *cfi;
    if(flags & 1) {
      for(const char* p = token; 1; ++p) {
        if(group_tokens_next >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end);
        *group_tokens_next++ = *p;
        if(!*p) break;
      }
    }
    else if(flags) {
      char* next; *vi = strtod(token, &next);
      if(next == token) *vi = numeric_limits<double>::quiet_NaN();
      ++vi;
    }
    ++cfi;
  }
}

void summarizer::process_line()
{
  if(first_line) {
    for(char* p = group_tokens; p < group_tokens_next; ++p) {
      out->process_token(p);
      while(*p) ++p;
    }
    out->process_line();
    first_line = 0;
    values = new double[num_data_columns];
  }
  else {
    if(group_tokens_next >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end);
    *group_tokens_next++ = '\x03';
    data_t::iterator i = data.find(group_tokens);
    if(i == data.end()) {
      size_t len = group_tokens_next - group_tokens;
      if(!group_storage_next || len >= size_t(group_storage_end - group_storage_next)) {
        if(group_storage_next) *group_storage_next++ = '\04';
        size_t cap = 256 * 1024;
        if(cap < len + 1) cap = len + 1;
        group_storage.push_back(new char[cap]);
        group_storage_next = group_storage.back();
        group_storage_end = group_storage.back() + cap;
      }
      memcpy(group_storage_next, group_tokens, len);
      if(!data_storage_next || num_data_columns > size_t(data_storage_end - data_storage_next)) {
        size_t rows = (256 * 1024) / (sizeof(summarizer_data_t) * num_data_columns);
        if(!rows) rows = 1;
        data_storage.push_back(new summarizer_data_t[rows * num_data_columns]);
        data_storage_next = data_storage.back();
        data_storage_end = data_storage.back() + (rows * num_data_columns);
      }
      i = data.insert(data_t::value_type(group_storage_next, data_storage_next)).first;
      group_storage_next += len;
      data_storage_next += num_data_columns;
    }
    vi = values;
    for(size_t c = 0; c < num_data_columns; ++c, ++vi) {
      summarizer_data_t& d = (*i).second[c];
      if(isnan(*vi)) ++d.missing;
      else {
        ++d.count;
        d.sum += *vi;
        d.sum_of_squares += *vi * *vi;
        if(*vi < d.min) d.min = *vi;
        else if(*vi > d.max) d.max = *vi;
      }
    }
  }

  cfi = column_flags.begin();
  vi = values;
  group_tokens_next = group_tokens;
}

void summarizer::process_stream()
{
  if(!out) throw runtime_error("summarizer has no out");

  *group_storage_next++ = '\x04';
  data.clear();

  size_t data_rows_per_storage = (256 * 1024) / (sizeof(summarizer_data_t) * num_data_columns);
  if(!data_rows_per_storage) data_rows_per_storage = 1;

  size_t row = 0;
  vector<char*>::const_iterator gsi = group_storage.begin();
  char* g = (gsi == group_storage.end()) ? 0 : *gsi;
  vector<summarizer_data_t*>::const_iterator dsi = data_storage.begin();
  summarizer_data_t* d = (dsi == data_storage.end()) ? 0 : *dsi;
  while(g && d) {
    while(*g != '\x03') { out->process_token(g); while(*g) ++g; ++g; }
    ++g;
    if(*g == '\x04') { delete[] *gsi; ++gsi; g = (gsi == group_storage.end()) ? 0 : *gsi; }

    for(cfi = column_flags.begin(); cfi != column_flags.end(); ++cfi) {
      if(!((*cfi) & 0xFFFFFFFE)) continue;

      if((*cfi) & SUM_MISSING) { sprintf(group_tokens, "%d", (*d).missing); out->process_token(group_tokens); }
      if((*cfi) & SUM_COUNT) { sprintf(group_tokens, "%d", (*d).count); out->process_token(group_tokens); }
      if((*cfi) & SUM_SUM) { sprintf(group_tokens, "%f", (*d).sum); out->process_token(group_tokens); }
      if((*cfi) & SUM_MIN) { sprintf(group_tokens, "%f", (*d).min); out->process_token(group_tokens); }
      if((*cfi) & SUM_MAX) { sprintf(group_tokens, "%f", (*d).max); out->process_token(group_tokens); }
      if((*cfi) & SUM_AVG) {
        if(!(*d).count) { out->process_token(""); }
        else { sprintf(group_tokens, "%f", ((*d).sum / (*d).count)); out->process_token(group_tokens); }
      }
      if((*cfi) & (SUM_VARIANCE | SUM_STD_DEV)) {
        double v = 0.0;
        if((*d).count > 1) {
          v = (*d).sum_of_squares - ((*d).sum * (*d).sum) / (*d).count;
          v /= (*d).count - 1;
        }
        if((*cfi) & SUM_VARIANCE) { sprintf(group_tokens, "%f", v); out->process_token(group_tokens); }
        if((*cfi) & SUM_STD_DEV) { sprintf(group_tokens, "%f", sqrt(v)); out->process_token(group_tokens); }
      }
      ++d;
    }
    if((++row % data_rows_per_storage) == (data_rows_per_storage - 1)) {
      delete[] *dsi; ++dsi; d = (dsi == data_storage.end()) ? 0 : *dsi;
    }

    out->process_line();
  }

  for(; gsi != group_storage.end(); ++gsi) delete[] *gsi;
  group_storage.clear();
  for(; dsi != data_storage.end(); ++dsi) delete[] *dsi;
  data_storage.clear();

  out->process_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// differ
////////////////////////////////////////////////////////////////////////////////////////////////

differ::differ() : out(0) {}
differ::differ(pass& out, const char* base_key, const char* comp_key, const char* keyword) { init(out, base_key, comp_key, keyword); }
differ::~differ() {}

void differ::init(pass& out, const char* base_key, const char* comp_key, const char* keyword)
{
  if(base_key < 0) throw runtime_error("invalid base_key");
  if(comp_key < 0) throw runtime_error("invalid comp_key");
  if(keyword < 0) throw runtime_error("invalid keyword");

  this->out = &out;
  this->base_key = base_key;
  this->comp_key = comp_key;
  this->keyword = keyword;
  first_row = 1;
  base_column = -1;
  comp_column = -1;
  column = 0;
  blank = 0;
}

void differ::process_token(const char* token)
{
  if(first_row) {
    if(!out) throw runtime_error("differ has no out");
    if(token == base_key) base_column = column;
    else if(token == comp_key) comp_column = column;
  }
  else {
    if(column == base_column) { char* next = 0; base_value = strtod(token, &next); if(next == token || *next) blank = 1; }
    else if(column == comp_column) { char* next = 0; comp_value = strtod(token, &next); if(next == token || *next) blank = 1; }
  }
  out->process_token(token);

  ++column;
}

void differ::process_line()
{
  if(first_row) {
    if(!out) throw runtime_error("differ has no out");
    if(base_column < 0) throw runtime_error("differ couldn't find the base column");
    if(comp_column < 0) throw runtime_error("differ couldn't find the comp column");
    out->process_token(keyword.c_str());
    first_row = 0;
  }
  else {
    if(blank) out->process_token("");
    else {
      char buf[32];
      sprintf(buf, "%f", comp_value - base_value);
      out->process_token(buf);
    }
  }
  out->process_line();
  column = 0;
  blank = 0;
}

void differ::process_stream()
{
  if(!out) throw runtime_error("differ has no out");
  out->process_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// bumper
////////////////////////////////////////////////////////////////////////////////////////////////

bumper::bumper() : values(0) { init(); }
bumper::bumper(pass& out) : values(0) { init(out); }
bumper::~bumper() { delete[] offsets; delete[] values; }

bumper& bumper::init()
{
  out = 0;
  insts.clear();
  first_line = 1;
  val_keys.clear();
  off_columns.clear();
  val_columns.clear();
  column = 0;
  oci = 0;
  vci = 0;
  delete[] offsets; offsets = 0;
  delete[] values; values = 0;

  return *this;
}

bumper& bumper::init(pass& out) { init(); return set_out(out); }
bumper& bumper::set_out(pass& out) { this->out = &out; return *this; }

bumper& bumper::add(const char* offset_key, const char* keys_to_offset_regex, bool sub)
{
  if(offset_key == 0) throw runtime_error("invalid offset_key");
  if(keys_to_offset_regex == 0) throw runtime_error("invalid keys_to_offset_regex");

  const char* err; int err_off; pcre* p = pcre_compile(keys_to_offset_regex, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("bumper can't compile keys_to_offset_regex");

  map<string, inst_t>::iterator i = insts.find(offset_key);
  if(i != insts.end()) {
    i = insts.insert(map<string, inst_t>::value_type(offset_key, inst_t())).first;
  }
  (*i).second.regex = p;
  (*i).second.sub = sub;
  (*i).second.off_column_index = numeric_limits<size_t>::max();

  return *this;
}


void bumper::process_token(const char* token)
{
  if(first_line) {
    if(!out) throw runtime_error("bumper has no out");
    map<string, inst_t>::iterator i = insts.find(token);
    if(i != insts.end()) {
      (*i).second.off_column_index = off_columns.size();
      off_columns.push_back(column);
      out->process_token(token);
    }
    else {
      size_t len = strlen(token);
      for(i = insts.begin(); i != insts.end(); ++i) {
        int ovector[30]; int rc = pcre_exec((*i).second.regex, 0, token, len, 0, 0, ovector, 30);
        if(rc >= 0) {
          val_keys.push_back(token);
          val_columns.resize(val_columns.size() + 1);
          val_columns.back().column = column;
          val_columns.back().inst = &(*i).second;
          break;
        }
        else {
          if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("bumper match error");
          else out->process_token(token);
        }
      }
    }
  }
  else {
    if(oci < off_columns.size() && column == off_columns[oci]) {
      char* next = 0; offsets[oci] = strtod(token, &next);
      if(next == token) offsets[oci] = numeric_limits<double>::quiet_NaN();
      out->process_token(token);
      ++oci;
    }
    else if(vci < val_columns.size() && column == val_columns[vci].column) {
      char* next = 0; values[vci] = strtod(token, &next);
      if(next == token) values[vci] = numeric_limits<double>::quiet_NaN();
      ++vci;
    }
    else out->process_token(token);
  }

  ++column;
}

void bumper::process_line()
{
  if(first_line) {
    if(!out) throw runtime_error("bumper has no out");

    first_line = 0;

    for(vector<string>::iterator i = val_keys.begin(); i != val_keys.end(); ++i) out->process_token((*i).c_str());
    val_keys.clear();

    offsets = new double[off_columns.size()];
    values = new double[val_columns.size()];
  }
  else {
    for(vci = 0; vci < val_columns.size(); ++vci) {
      double v = values[vci];
      if(isnan(v)) out->process_token("");
      else {
        const inst_t& inst = *val_columns[vci].inst;
        double o = inst.off_column_index < off_columns.size() ? offsets[inst.off_column_index] : numeric_limits<double>::quiet_NaN();
        if(!isnan(o)) v = inst.sub ? v - o : v + o;

        char buf[32];
        sprintf(buf, "%f", v);
        out->process_token(buf);
      }
    }
  }
  out->process_line();
  column = 0;
  oci = 0;
  vci = 0;
}

void bumper::process_stream()
{
  if(!out) throw runtime_error("bumper has no out");
  out->process_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// base_converter
////////////////////////////////////////////////////////////////////////////////////////////////

base_converter::base_converter() { init(); }
base_converter::base_converter(pass& out, const char* regex, int from, int to) { init(out, regex, from, to); }
base_converter::~base_converter() {}

base_converter& base_converter::init()
{
  this->out = 0;
  regex_base_conv.clear();
  first_row = 1;
  column = 0;
  from_base.clear();
  to_base.clear();
  return *this;
}

base_converter& base_converter::init(pass& out, const char* regex, int from, int to) { init(); set_out(out); return add_conv(regex, from, to); }
base_converter& base_converter::set_out(pass& out) { this->out = &out; return *this; }

base_converter& base_converter::add_conv(const char* regex, int from, int to)
{
  if(from != 8 && from != 10 && from != 16) throw runtime_error("from isn't recognized");
  if(to != 8 && to != 10 && to != 16) throw runtime_error("from isn't recognized");

  const char* err; int err_off; pcre* p = pcre_compile(regex, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("base_converter can't compile regex");
  regex_base_conv.resize(regex_base_conv.size() + 1);
  regex_base_conv.back().regex = p;
  regex_base_conv.back().from = from;
  regex_base_conv.back().to = to;

  return *this;
}

void base_converter::process_token(const char* token)
{
  if(first_row) {
    if(!out) throw runtime_error("differ has no out");
    size_t len = strlen(token);
    int from = -1; int to = 0;
    for(vector<regex_base_conv_t>::const_iterator i = regex_base_conv.begin(); i != regex_base_conv.end(); ++i) {
      int ovector[30]; int rc = pcre_exec((*i).regex, 0, token, len, 0, 0, ovector, 30);
      if(rc >= 0) { from = (*i).from; to = (*i).to; break; }
      else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("base_converter match error");
    }
    from_base.push_back(from); to_base.push_back(to);
    out->process_token(token);
  }
  else if(from_base[column] < 0) out->process_token(token);
  else {
    char* next = 0;
    long int ivalue = 0;
    double dvalue = 0.0;
    if(from_base[column] == 10) { dvalue = strtod(token, &next); ivalue = (long int)dvalue; }
    else { ivalue = strtol(token, &next, from_base[column]); dvalue = ivalue; }

    if(next == token) out->process_token(token);
    else {
      char buf[256];
      if(to_base[column] == 10) { sprintf(buf, "%f", dvalue); out->process_token(buf); }
      else if(to_base[column] == 8) { sprintf(buf, "%#lo", ivalue); out->process_token(buf); }
      else if(to_base[column] == 16) { sprintf(buf, "%#lx", ivalue); out->process_token(buf); }
      else out->process_token(token);
    }
  }

  ++column;
}

void base_converter::process_line() {
  if(first_row) {
    if(!out) throw runtime_error("differ has no out");
    first_row = 0;
  }
  column = 0;
  out->process_line();
}

void base_converter::process_stream() { out->process_stream(); }


////////////////////////////////////////////////////////////////////////////////////////////////
// variance_analyzer
////////////////////////////////////////////////////////////////////////////////////////////////

variance_analyzer::variance_analyzer() : group_tokens(0), values(0) { init(); }
variance_analyzer::variance_analyzer(pass& out) : group_tokens(0), values(0)  { init(out); }

variance_analyzer::~variance_analyzer()
{
  for(vector<pcre*>::iterator i = group_regexes.begin(); i != group_regexes.end(); ++i) pcre_free(*i);
  for(vector<pcre*>::iterator i = data_regexes.begin(); i != data_regexes.end(); ++i) pcre_free(*i);
  for(vector<pcre*>::iterator i = exception_regexes.begin(); i != exception_regexes.end(); ++i) pcre_free(*i);
  delete[] group_tokens;
  delete[] values;
  for(vector<variance_analyzer_treatment_data_t*>::iterator i = data.begin(); i != data.end(); ++i) delete[] *i;
}

variance_analyzer& variance_analyzer::init()
{
  this->out = 0;
  for(vector<pcre*>::iterator i = group_regexes.begin(); i != group_regexes.end(); ++i) pcre_free(*i);
  group_regexes.clear();
  for(vector<pcre*>::iterator i = data_regexes.begin(); i != data_regexes.end(); ++i) pcre_free(*i);
  data_regexes.clear();
  for(vector<pcre*>::iterator i = exception_regexes.begin(); i != exception_regexes.end(); ++i) pcre_free(*i);
  exception_regexes.clear();
  first_line = 1;
  column_type.clear();

  delete[] group_tokens; group_tokens = new char[2048];
  group_tokens_next = group_tokens;
  group_tokens_end = group_tokens + 2048;
  group_storage.clear();
  group_storage_next = 0;
  groups.clear();
  delete[] values; values = 0;
  for(vector<variance_analyzer_treatment_data_t*>::iterator i = data.begin(); i != data.end(); ++i) delete[] *i;
  data.clear();

  return *this;
}

variance_analyzer& variance_analyzer::init(pass& out) { init(); return set_out(out); }
variance_analyzer& variance_analyzer::set_out(pass& out) { this->out = &out; return *this; }

variance_analyzer& variance_analyzer::add_group(const char* regex)
{
  const char* err; int err_off; pcre* p = pcre_compile(regex, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("variance_analyzer can't compile group regex");
  group_regexes.push_back(p);
  return *this;
}

variance_analyzer& variance_analyzer::add_data(const char* regex)
{
  const char* err; int err_off; pcre* p = pcre_compile(regex, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("variance_analyzer can't compile data regex");
  data_regexes.push_back(p);
  return *this;
}

variance_analyzer& variance_analyzer::add_exception(const char* regex)
{
  const char* err; int err_off; pcre* p = pcre_compile(regex, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("variance_analyzer can't compile exception regex");
  exception_regexes.push_back(p);
  return *this;
}

void variance_analyzer::process_token(const char* token)
{
  if(first_line) {
    if(!out) throw runtime_error("variance_analyzer has no out");
    char type = 0;
    size_t len = strlen(token);
    for(vector<pcre*>::iterator j = group_regexes.begin(); j != group_regexes.end(); ++j) {
      int ovector[30]; int rc = pcre_exec(*j, 0, token, len, 0, 0, ovector, 30);
      if(rc >= 0) { type = 1; break; }
      else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("variance_analyzer match error");
    }
    if(!type) {
      for(vector<pcre*>::iterator j = data_regexes.begin(); j != data_regexes.end(); ++j) {
        int ovector[30]; int rc = pcre_exec(*j, 0, token, len, 0, 0, ovector, 30);
        if(rc >= 0) { type = 2; break; }
        else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("variance_analyzer match error");
      }
    }
    if(type) {
      for(vector<pcre*>::iterator j = exception_regexes.begin(); j != exception_regexes.end(); ++j) {
        int ovector[30]; int rc = pcre_exec(*j, 0, token, len, 0, 0, ovector, 30);
        if(rc >= 0) { type = 0; break; }
        else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("variance_analyzer match error");
      }
    }
    column_type.push_back(type);
    if(type == 2) data_keywords.push_back(token);
  }
  else {
    if(cti == column_type.end()) return;

    if(*cti == 1) {
      for(const char* p = token; 1; ++p) {
        if(group_tokens_next >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end);
        *group_tokens_next++ = *p;
        if(!*p) break;
      }
    }
    else if(*cti == 2) {
      char* next; *vi = strtod(token, &next);
      if(next == token) *vi = numeric_limits<double>::quiet_NaN();
      ++vi;
    }
    ++cti;
  }
}

void variance_analyzer::process_line()
{
  if(first_line) {
    if(!out) throw runtime_error("variance_analyzer has no out");
    first_line = 0;
    values = new double[data_keywords.size()];
  }
  else {
    if(group_tokens_next >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end);
    *group_tokens_next++ = '\x03';

    groups_t::iterator gi = groups.find(group_tokens);
    if(gi == groups.end()) {
      size_t len = group_tokens_next - group_tokens;
      if(!group_storage_next || len + 1 > size_t(group_storage_end - group_storage_next)) {
        if(group_storage_next) *group_storage_next++ = '\x04';
        size_t cap = 256 * 1024;
        if(cap < len + 1) cap = len + 1;
        group_storage.push_back(new char[cap]);
        group_storage_next = group_storage.back();
        group_storage_end = group_storage.back() + cap;
      }
      memcpy(group_storage_next, group_tokens, len);
      gi = groups.insert(groups_t::value_type(group_storage_next, groups.size())).first;
      group_storage_next += len;
      data.push_back(new variance_analyzer_treatment_data_t[data_keywords.size()]);
    }
    vi = values;
    variance_analyzer_treatment_data_t* d = data[(*gi).second];
    for(size_t i = 0; i < data_keywords.size(); ++i, ++vi, ++d) {
      if(!isnan(*vi)) {
        ++d->count;
        d->sum += *vi;
        d->sum_of_squares += *vi * *vi;
      }
    }
  }

  cti = column_type.begin();
  group_tokens_next = group_tokens;
  vi = values;
}

void variance_analyzer::process_stream()
{
  if(!out) throw runtime_error("variance_analyzer has no out");

  if(group_storage_next) *group_storage_next++ = '\x04';

  out->process_token("keyword");
  vector<char*>::const_iterator gsi = group_storage.begin();
  char* gsp = gsi == group_storage.end() ? 0 : *gsi;
  while(gsp) {
    group_tokens_next = group_tokens + 8;
    for(; 1; ++gsp) {
      if(!*gsp) { *group_tokens_next++ = ' '; }
      else if(*gsp == '\x03') { --group_tokens_next; *group_tokens_next++ = ')'; *group_tokens_next++ = '\0'; break; }
      else { *group_tokens_next++ = *gsp; }
    }
    memcpy(group_tokens + 4, "AVG(", 4); out->process_token(group_tokens + 4);
    memcpy(group_tokens, "STD_DEV(", 8); out->process_token(group_tokens);
    if(*++gsp == '\x04') { ++gsi; gsp = gsi == group_storage.end() ? 0 : *gsi; }
  }
  out->process_token("AVG");
  out->process_token("STD_DEV");
  out->process_token("f");
  out->process_token("p");
  out->process_line();

  const size_t max_groups = groups.size();
  for(gsi = group_storage.begin(); gsi != group_storage.end(); ++gsi) delete[] *gsi;
  groups.clear();

  size_t di = 0;
  for(vector<string>::const_iterator dki = data_keywords.begin(); dki != data_keywords.end(); ++dki, ++di) {
    out->process_token((*dki).c_str());

    size_t num_groups = max_groups;
    int total_count = 0;
    double total_sum = 0.0;
    double sum_of_sum_of_squares = 0.0;
    double sum_of_sum_squared_over_count = 0.0;
    for(size_t gi = 0; gi < max_groups; ++gi) {
      variance_analyzer_treatment_data_t* dp = data[gi] + di;

      if(!dp->count) --num_groups;
      total_count += dp->count;
      total_sum += dp->sum;
      sum_of_sum_of_squares += dp->sum_of_squares;
      sum_of_sum_squared_over_count += (dp->sum * dp->sum) / dp->count;

      if(dp->count) sprintf(group_tokens, "%f", dp->sum / dp->count);
      else group_tokens[0] = '\0';
      out->process_token(group_tokens);
      if(dp->count > 1) sprintf(group_tokens, "%f", sqrt((dp->sum_of_squares - ((dp->sum * dp->sum) / dp->count)) / (dp->count - 1)));
      else group_tokens[0] = '\0';
      out->process_token(group_tokens);
    }

    if(num_groups > 1) {
      double total_sum_squared_over_total_count = ((total_sum * total_sum) / total_count);
      double sst = sum_of_sum_of_squares - total_sum_squared_over_total_count;
      //double sst_df = (total_count - 1);
      double sstr = sum_of_sum_squared_over_count - total_sum_squared_over_total_count;
      double sstr_df = (num_groups - 1);
      double sse = sst - sstr;
      double sse_df = (total_count - num_groups);
      double f = (sstr / sstr_df) / (sse / sse_df);
      if(isnan(f))
        f = numeric_limits<double>::infinity();
      double p = 1.0;
      if(f != numeric_limits<double>::infinity())
        p = ibeta(sstr_df / 2, sse_df / 2, (sstr_df * f) / (sstr_df * f + sse_df));

      sprintf(group_tokens, "%f", total_sum / total_count); out->process_token(group_tokens);
      sprintf(group_tokens, "%f", sqrt(sst / (total_count - 1))); out->process_token(group_tokens);
      sprintf(group_tokens, "%f", f); out->process_token(group_tokens);
      sprintf(group_tokens, "%f", p); out->process_token(group_tokens);
    }
    else {
      group_tokens[0] = '\0';
      out->process_token(group_tokens);
      out->process_token(group_tokens);
      out->process_token(group_tokens);
      out->process_token(group_tokens);
    }

    out->process_line();
  }

  for(data_t::iterator di = data.begin(); di != data.end(); ++di) delete[] *di;
  data.clear();

  out->process_stream();
}


}
