#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <limits>
#include <algorithm>
#include <stdexcept>
#include <math.h>
#include "table.h"

using namespace std;
#ifdef use_unordered
using namespace std::tr1;
#endif


namespace table {

void resize_buffer(char*& buf, char*& next, char*& end, char** resize_end)
{
  char* old = buf;
  size_t size = next - buf;
  size_t cap = end - buf;
  size_t resize = resize_end ? end - *resize_end : 0;

  size_t new_cap = cap * 2;
  buf = new char[new_cap];
  memcpy(buf, old, size);
  delete[] old;

  next = buf + size;
  end = buf + new_cap;
  if(resize_end) *resize_end = end - resize;
}


////////////////////////////////////////////////////////////////////////////////////////////////
// cstring_queue
////////////////////////////////////////////////////////////////////////////////////////////////

vector<cstring_queue::block_t> cstring_queue::free_blocks;

vector<cstring_queue::block_t>::iterator cstring_queue::add_block(size_t len)
{
  vector<block_t>::iterator fbi = free_blocks.begin();
  while(fbi != free_blocks.end() && (*fbi).data->cap <= len) ++fbi;

  vector<block_t>::iterator i;
  if(fbi != free_blocks.end()) {
    if(!data->blocks.size()) data->next_read = (*fbi).data->buf;
    i = data->blocks.insert(data->blocks.end(), *fbi);
    free_blocks.erase(fbi);
    //cout << "found_block " << len << ' ' << (*i).data->cap << endl;
  }
  else {
    size_t cap = default_block_cap;
    if(len >= cap) cap = len + 1;
    block_t b(cap);
    if(!data->blocks.size()) data->next_read = b.data->buf;
    i = data->blocks.insert(data->blocks.end(), b);
    //cout << "new_block " << len << ' ' << (*i).data->cap << endl;
  }

  (*i).data->next_write = (*i).data->buf;

  return i;
}

void cstring_queue::copy()
{
  if(data->rc <= 1) { cow = 0; return; }

  cstring_queue_data_t* old = data;
  data = new cstring_queue_data_t;
  data->rc = 1;
  for(vector<block_t>::const_iterator i = old->blocks.begin(); i != old->blocks.end(); ++i) {
    block_t t((*i).data->cap);
    memcpy(t.data->buf, (*i).data->buf, (*i).data->next_write - (*i).data->buf);
    t.data->last_token = (*i).data->last_token;
    t.data->next_write = (*i).data->next_write;
    data->blocks.push_back(t);
  }
  data->next_read = old->next_read;
  cow = 0;
}

void cstring_queue::clear()
{
  if(data->rc <= 1) {
    for(vector<block_t>::iterator i = data->blocks.begin(); i != data->blocks.end(); ++i)
      free_blocks.push_back(*i);
    data->blocks.clear();
    data->next_read = 0;
  }
  else {
    cstring_queue_data_t* old = data;
    --old->rc;
    data = new cstring_queue_data_t;
    data->rc = 1;
    data->next_read = 0;
  }
  cow = 0;
}


////////////////////////////////////////////////////////////////////////////////////////////////
// pass
////////////////////////////////////////////////////////////////////////////////////////////////

pass::~pass() {}


////////////////////////////////////////////////////////////////////////////////////////////////
// subset_tee
////////////////////////////////////////////////////////////////////////////////////////////////

subset_tee_dest_data_t::~subset_tee_dest_data_t()
{
  for(vector<pcre*>::iterator i = regex.begin(); i != regex.end(); ++i) pcre_free(*i);
  for(vector<pcre*>::iterator i = regex_except.begin(); i != regex_except.end(); ++i) pcre_free(*i);
}

subset_tee::subset_tee() { init(); }
subset_tee::subset_tee(pass& dest) { init(dest); }

subset_tee& subset_tee::init() {
  dest_data.clear();
  ddi = dest_data.end();
  first_row = 1;
  column = 0;
  this->dest.clear();
  return *this;
}

subset_tee& subset_tee::init(pass& dest) { init(); return set_dest(dest); }
subset_tee& subset_tee::set_dest(pass& dest) { ddi = dest_data.insert(map<pass*, subset_tee_dest_data_t>::value_type(&dest, subset_tee_dest_data_t())).first; return *this; }

subset_tee& subset_tee::add_data(bool regex, const char* key)
{
  if(ddi == dest_data.end()) throw runtime_error("no current destination");

  if(regex) {
    const char* err; int err_off; pcre* p = pcre_compile(key, 0, &err, &err_off, 0);
    if(!p) throw runtime_error("subset_tee can't compile data regex");
    (*ddi).second.regex.push_back(p);
  }
  else (*ddi).second.key.insert(key);

  return *this;
}

subset_tee& subset_tee::add_exception(bool regex, const char* key)
{
  if(ddi == dest_data.end()) throw runtime_error("no current destination");

  if(regex) {
    const char* err; int err_off; pcre* p = pcre_compile(key, 0, &err, &err_off, 0);
    if(!p) throw runtime_error("subset_tee can't compile exception regex");
    (*ddi).second.regex_except.push_back(p);
  }
  else (*ddi).second.key_except.insert(key);

  return *this;
}

void subset_tee::process_token(const char* token)
{
  if(first_row) {
    for(std::map<pass*, subset_tee_dest_data_t>::iterator di = dest_data.begin(); di != dest_data.end(); ++di) {
      subset_tee_dest_data_t& d = (*di).second;

      size_t len = strlen(token);
      bool add = 0;
      set<string>::const_iterator ki = d.key.find(token);
      if(ki != d.key.end()) add = 1;
      else {
        for(vector<pcre*>::const_iterator ri = d.regex.begin(); ri != d.regex.end(); ++ri) {
          int ovector[30]; int rc = pcre_exec(*ri, 0, token, len, 0, 0, ovector, 30);
          if(rc >= 0) { add = 1; break; }
          else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("subset_tee match error");
        }
      }

      set<string>::const_iterator kei = d.key_except.find(token);
      if(kei != d.key_except.end()) add = 0;
      else {
        for(vector<pcre*>::const_iterator ri = d.regex_except.begin(); ri != d.regex_except.end(); ++ri) {
          int ovector[30]; int rc = pcre_exec(*ri, 0, token, len, 0, 0, ovector, 30);
          if(rc >= 0) { add = 0; break; }
          else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("subset_tee match error");
        }
      }

      if(add) {
        dest.resize(dest.size() + 1);
        dest.back().first = column;
        dest.back().second = (*di).first;
        d.has_data = 1;
        (*di).first->process_token(token);
      }
    }
  }
  else {
    for(; di != dest.end() && (*di).first == column; ++di)
      (*di).second->process_token(token);
  }

  ++column;
}

void subset_tee::process_line()
{
  first_row = 0;
  for(std::map<pass*, subset_tee_dest_data_t>::iterator ddi = dest_data.begin(); ddi != dest_data.end(); ++ddi)
    if((*ddi).second.has_data)
      (*ddi).first->process_line();
  column = 0;
  di = dest.begin();
}

void subset_tee::process_stream()
{
  for(std::map<pass*, subset_tee_dest_data_t>::iterator ddi = dest_data.begin(); ddi != dest_data.end(); ++ddi)
    (*ddi).first->process_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// ordered_tee
////////////////////////////////////////////////////////////////////////////////////////////////

ordered_tee::ordered_tee() {}
ordered_tee::ordered_tee(pass& out1, pass& out2) { init(out1, out2); }
ordered_tee::~ordered_tee() {}

void ordered_tee::init(pass& out1, pass& out2)
{
  out.clear();
  first_row = 1;
  num_columns = 0;
  data.clear();
  data.set_default_cap(256 * 1024);
  out.push_back(&out1);
  out.push_back(&out2);
}

void ordered_tee::add_out(pass& out) { this->out.push_back(&out); }

void ordered_tee::process_token(const char* token)
{
  if(!out.size()) throw runtime_error("ordered_tee::process_token no outs");
  out[0]->process_token(token);

  if(first_row) ++num_columns;

  data.push(token);
}

void ordered_tee::process_line()
{
  if(!out.size()) throw runtime_error("ordered_tee::process_line no outs");

  out[0]->process_line();
  first_row = 0;
}

void ordered_tee::process_stream()
{
  vector<pass*>::iterator oi = out.begin();
  (*oi)->process_stream();
  for(++oi; oi != out.end(); ++oi) {
    vector<pass*>::iterator noi = oi; ++noi;
    const bool last_out = noi == out.end();

    int column = 0;
    if(!last_out) {
      for(cstring_queue::const_iterator i = data.begin(); i != data.end(); ++i) {
        (*oi)->process_token(*i);
        data.pop();
        if(++column >= num_columns) {
          (*oi)->process_line();
          column = 0;
        }
      }
    }
    else {
      while(!data.empty()) {
        (*oi)->process_token(data.front());
        data.pop();
        if(++column >= num_columns) {
          (*oi)->process_line();
          column = 0;
        }
      }
    }
    (*oi)->process_stream();
  }
  data.clear();
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
// stacker
////////////////////////////////////////////////////////////////////////////////////////////////

stacker::stacker(stack_action_e default_action) { init(default_action); }
stacker::stacker(pass& out, stack_action_e default_action) { init(out, default_action); }
stacker::~stacker() {}

stacker& stacker::re_init()
{
  first_line = 1;
  stack_keys.clear();
  actions.clear();
  last_leave = 0;

  column = 0;
  stack_column = 0;
  leave_tokens.clear();
  stack_tokens.clear();
  return *this;
}

stacker& stacker::init(stack_action_e default_action)
{
  this->out = 0;
  this->default_action = default_action;
  keyword_actions.clear();
  regex_actions.clear();
  re_init();
  return *this;
}

stacker& stacker::init(pass& out, stack_action_e default_action) { init(default_action); return set_out(out); }
stacker& stacker::set_out(pass& out) { this->out = &out; return *this; }

stacker& stacker::add_action(bool regex, const char* key, stack_action_e action)
{
  if(regex) {
    const char* err; int err_off; pcre* p = pcre_compile(key, 0, &err, &err_off, 0);
    if(!p) throw runtime_error("stacker can't compile group regex");
    regex_actions.resize(regex_actions.size() + 1);
    regex_actions.back().regex = p;
    regex_actions.back().action = action;
  }
  else keyword_actions[key] = action;

  return *this;
}

void stacker::process_token(const char* token)
{
  if(first_line) {
    if(!out) throw runtime_error("stacker has no out");
    stack_action_e action = default_action;
    map<string, stack_action_e>::iterator i = keyword_actions.find(token);
    if(i != keyword_actions.end()) action = (*i).second;
    else {
      size_t len = strlen(token);
      for(vector<regex_stack_action_t>::iterator j = regex_actions.begin(); j != regex_actions.end(); ++j) {
        int ovector[30]; int rc = pcre_exec((*j).regex, 0, token, len, 0, 0, ovector, 30);
        if(rc >= 0) { action = (*j).action; break; }
        else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("stacker match error");
      }
    }
    actions.push_back(action);
    if(action == ST_LEAVE) { leave_tokens.push(token); last_leave = column; out->process_token(token); }
    else if(action == ST_STACK) stack_keys.push_back(token);
  }
  else {
    if(column >= actions.size()) throw runtime_error("too many columns");
    else if(actions[column] == ST_LEAVE) {
      leave_tokens.push(token);
      if(column == last_leave) {
        for(stack_column = 0; !stack_tokens.empty(); ++stack_column) {
          for(cstring_queue::const_iterator i = leave_tokens.begin(); i != leave_tokens.end(); ++i) { out->process_token(*i); }
          out->process_token(stack_keys[stack_column].c_str());
          out->process_token(stack_tokens.front());
          stack_tokens.pop();
          out->process_line();
        }
      }
    }
    else if(actions[column] == ST_STACK) { 
      if(column < last_leave) stack_tokens.push(token);
      else {
        for(cstring_queue::const_iterator i = leave_tokens.begin(); i != leave_tokens.end(); ++i) out->process_token(*i);
        out->process_token(stack_keys[stack_column++].c_str());
        out->process_token(token);
        out->process_line();
      }
    }
  }

  ++column;
}

void stacker::process_line()
{
  if(first_line) {
    if(!out) throw runtime_error("stacker has no out");
    out->process_token("keyword");
    out->process_token("data");
    out->process_line();
    first_line = 0;
  }
  column = 0;
  stack_column = 0;
  leave_tokens.clear();
}

void stacker::process_stream()
{
  if(!out) throw runtime_error("stacker has no out");
  out->process_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// splitter
////////////////////////////////////////////////////////////////////////////////////////////////

splitter::splitter(split_action_e default_action) : group_tokens(0), split_by_tokens(0), split_tokens(0) { init(default_action); }
splitter::splitter(pass& out, split_action_e default_action) : group_tokens(0), split_by_tokens(0), split_tokens(0)  { init(out, default_action); }

splitter& splitter::init(split_action_e default_action)
{
  this->out = 0;
  this->default_action = default_action;
  keyword_actions.clear();
  regex_actions.clear();

  first_line = 1;
  actions.clear();
  split_keys.clear();

  column = 0;
  delete[] group_tokens;
  group_tokens = new char[2048];
  group_tokens_next = group_tokens;
  group_tokens_end = group_tokens + 2048;
  delete[] split_by_tokens;
  split_by_tokens = new char[2048];
  split_by_tokens_next = split_by_tokens;
  split_by_tokens_end = split_by_tokens + 2048;
  delete[] split_tokens;
  split_tokens = new char[2048];
  split_tokens_next = split_tokens;
  split_tokens_end = split_tokens + 2048;

  for(map<char*, size_t, cstr_less>::const_iterator i = out_split_keys.begin(); i != out_split_keys.end(); ++i)
    delete[] (*i).first;
  for(vector<char*>::const_iterator i = group_storage.begin(); i != group_storage.end(); ++i)
    delete[] *i;
  group_storage_next = 0;
  group_storage_end = 0;
  out_split_keys.clear();
  data.clear();

  return *this;
}

splitter& splitter::init(pass& out, split_action_e default_action) { init(default_action); return set_out(out); }

splitter::~splitter()
{
  delete[] group_tokens;
  delete[] split_by_tokens;
  delete[] split_tokens;
  for(map<char*, size_t, cstr_less>::const_iterator i = out_split_keys.begin(); i != out_split_keys.end(); ++i)
    delete[] (*i).first;
  for(vector<char*>::const_iterator i = group_storage.begin(); i != group_storage.end(); ++i)
    delete[] *i;
}

splitter& splitter::set_out(pass& out) { this->out = &out; return *this; }

splitter& splitter::add_action(bool regex, const char* key, split_action_e action)
{
  if(regex) {
    const char* err; int err_off; pcre* p = pcre_compile(key, 0, &err, &err_off, 0);
    if(!p) throw runtime_error("splitter can't compile data regex");
    regex_actions.resize(regex_actions.size() + 1);
    regex_actions.back().regex = p;
    regex_actions.back().action = action;
  }
  else keyword_actions[key] = action;

  return *this;
}

void splitter::process_token(const char* token)
{
  if(first_line) {
    if(!out) throw runtime_error("splitter has no out");
    split_action_e action = default_action;
    map<string, split_action_e>::iterator i = keyword_actions.find(token);
    if(i != keyword_actions.end()) action = (*i).second;
    else {
      size_t len = strlen(token);
      for(vector<regex_split_action_t>::iterator j = regex_actions.begin(); j != regex_actions.end(); ++j) {
        int ovector[30]; int rc = pcre_exec((*j).regex, 0, token, len, 0, 0, ovector, 30);
        if(rc >= 0) { action = (*j).action; break; }
        else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("splitter match error");
      }
    }
    actions.push_back(action);
    if(action == SP_GROUP) group_keys.push_back(token);
    else if(action == SP_SPLIT) split_keys.push_back(token);
  }
  else {
    if(actions[column] == SP_GROUP) {
      for(const char* p = token; 1; ++p) {
        if(group_tokens_next >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end);
        *group_tokens_next++ = *p;
        if(!*p) break;
      }
    }
    else if(actions[column] == SP_SPLIT_BY) {
      for(const char* p = token; 1; ++p) {
        if(split_by_tokens_next >= split_by_tokens_end) resize_buffer(split_by_tokens, split_by_tokens_next, split_by_tokens_end);
        *split_by_tokens_next++ = *p;
        if(!*p) break;
      }
    }
    else if(actions[column] == SP_SPLIT) {
      for(const char* p = token; 1; ++p) {
        if(split_tokens_next >= split_tokens_end) resize_buffer(split_tokens, split_tokens_next, split_tokens_end);
        *split_tokens_next++ = *p;
        if(!*p) break;
      }
    }
  }

  ++column;
}

void splitter::process_line()
{
  if(first_line) {
    if(!out) throw runtime_error("splitter has no out");
    first_line = 0;
  }
  else {
    if(group_tokens_next >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end);
    *group_tokens_next++ = '\x03';
    if(split_by_tokens_next >= split_by_tokens_end) resize_buffer(split_by_tokens, split_by_tokens_next, split_by_tokens_end);
    *split_by_tokens_next++ = '\x03';
    if(split_tokens_next >= split_tokens_end) resize_buffer(split_tokens, split_tokens_next, split_tokens_end);
    *split_tokens_next++ = '\x03';

    data_t::iterator di = data.find(group_tokens);
    if(di == data.end()) {
      size_t len = group_tokens_next - group_tokens;
      if(len + 1 > size_t(group_storage_end - group_storage_next)) {
        if(group_storage_next) *group_storage_next++ = '\x04';
        size_t cap = 256 * 1024;
        if(cap < len + 1) cap = len + 1;
        group_storage.push_back(new char[cap]);
        group_storage_next = group_storage.back();
        group_storage_end = group_storage.back() + cap;
      }
      memcpy(group_storage_next, group_tokens, len);
      di = data.insert(data_t::value_type(group_storage_next, vector<string>())).first;
      group_storage_next += len;
    }
    vector<string>& values = (*di).second;
    vector<string>::const_iterator ski = split_keys.begin();
    for(char* stp = split_tokens; *stp != '\x03'; ++ski, ++stp) {
      group_tokens_next = group_tokens;
      for(const char* p = (*ski).c_str(); *p; ++p) {
        if(group_tokens_next >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end);
        *group_tokens_next++ = *p;
      }
      *group_tokens_next++ = ' ';
      size_t sk_len = group_tokens_next - group_tokens;
      for(char* sbtp = split_by_tokens; *sbtp != '\x03'; ++sbtp) {
        group_tokens_next = group_tokens + sk_len;
        for(; 1; ++sbtp) {
          if(group_tokens_next >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end);
          *group_tokens_next++ = *sbtp;
          if(!*sbtp) break;
        }
      }

      map<char*, size_t, cstr_less>::iterator i = out_split_keys.find(group_tokens);
      if(i == out_split_keys.end()) {
        char* cpy = new char[group_tokens_next - group_tokens];
        memcpy(cpy, group_tokens, group_tokens_next - group_tokens);
        i = out_split_keys.insert(map<char*, size_t, cstr_less>::value_type(cpy, out_split_keys.size())).first;
      }
      const size_t& index = (*i).second;
      if(index >= values.size()) values.resize(index + 1);
      values[index] = stp;

      while(*stp) ++stp;
    }
  }

  column = 0;
  group_tokens_next = group_tokens;
  split_by_tokens_next = split_by_tokens;
  split_tokens_next = split_tokens;
}

void splitter::process_stream()
{
  if(!out) throw runtime_error("splitter has no out");

  for(vector<string>::const_iterator i = group_keys.begin(); i != group_keys.end(); ++i)
    out->process_token((*i).c_str());
  {
    vector<char*> osk(out_split_keys.size());
    for(map<char*, size_t, cstr_less>::const_iterator i = out_split_keys.begin(); i != out_split_keys.end(); ++i)
      osk[(*i).second] = (*i).first;
    for(vector<char*>::const_iterator i = osk.begin(); i != osk.end(); ++i)
      out->process_token(*i);
  }
  out->process_line();

  map<char*, size_t, cstr_less>::size_type num_out_split_keys = out_split_keys.size();
  for(map<char*, size_t, cstr_less>::const_iterator i = out_split_keys.begin(); i != out_split_keys.end(); ++i)
    delete[] (*i).first;
  out_split_keys.clear();

  for(data_t::const_iterator i = data.begin(); i != data.end(); ++i) {
    for(char* p = (*i).first; *p != '\x03'; ++p) {
      out->process_token(p);
      while(*p) ++p;
    }
    size_t tokens = 0;
    for(vector<string>::const_iterator j = (*i).second.begin(); j != (*i).second.end(); ++j, ++tokens)
      out->process_token((*j).c_str());
    while(tokens++ < num_out_split_keys)
      out->process_token("");
    out->process_line();
  }

  for(vector<char*>::const_iterator i = group_storage.begin(); i != group_storage.end(); ++i)
    delete[] *i;
  group_storage.clear();
  data.clear();

  out->process_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// row_joiner
////////////////////////////////////////////////////////////////////////////////////////////////

row_joiner::row_joiner() : out(0) {}
row_joiner::row_joiner(pass& out, const char* table_name) { init(out, table_name); }

row_joiner& row_joiner::init(pass& out, const char* table_name)
{
  this->out = &out;
  this->table_name.clear();
  table = 0;
  first_line = 1;
  more_lines = 0;
  column = 0;
  num_columns.clear();
  keys.clear();
  data.clear();
  add_table_name(table_name);
  return *this;
}

row_joiner& row_joiner::add_table_name(const char* name)
{
  if(name) table_name.push_back(name);
  else {
    stringstream ss; ss << "Table " << table_name.size();
    table_name.push_back(ss.str());
  }
  num_columns.push_back(0);

  return *this;
}

void row_joiner::process_token(const char* token)
{
  if(!first_line) data[table].push(token);
  else {
    if(!more_lines) {
      if(table >= table_name.size()) add_table_name();
      ++num_columns[table];
      keys.push_back(token);
    }
    else {
      size_t c = column;
      for(size_t t = 0; t < table; ++t)
        c += num_columns[t];
      if(keys[c].compare(token) && keys[c].compare(string(token) + " of " + table_name[table]))
        throw runtime_error("column keys don't match previous tables");
      ++column;
    }
  }
}

void row_joiner::process_line()
{
  if(first_line) {
    first_line = 0;
    data.resize(data.size() + 1);
    if(more_lines) {
      if(column != num_columns[table]) throw runtime_error("num_columns doesn't match");
      column = 0;
    }
  }
}

void row_joiner::process_stream()
{
  ++table;
  first_line = 1;
}

void row_joiner::process_lines()
{
  if(!out) throw runtime_error("row_joiner has no out");

  if(!more_lines) {
    for(vector<string>::iterator ki = keys.begin(); ki != keys.end(); ++ki) {
      bool found = 0;
      vector<string>::iterator nki = ki; ++nki;
      while(1) {
        nki = find(nki, keys.end(), *ki);
        if(nki == keys.end()) break;

        found = 1;
        size_t keyi = nki - keys.begin();
        size_t t = 0;
        for(; t < num_columns.size(); ++t) {
          if(keyi < num_columns[t]) break;
          else keyi -= num_columns[t];
        }
        (*nki) = (*nki) + " of " + table_name[t];
      }

      if(found) {
        size_t keyi = ki - keys.begin();
        size_t t = 0;
        for(; t < num_columns.size(); ++t) {
          if(keyi < num_columns[t]) break;
          else keyi -= num_columns[t];
        }
        (*ki) = (*ki) + " of " + table_name[t];
      }
      out->process_token((*ki).c_str());
    }
    out->process_line();
  }

  vector<size_t>::const_iterator nci = num_columns.begin();
  vector<cstring_queue>::iterator di = data.begin();
  while(!(*di).empty()) {
    for(size_t col = 0; col < (*nci) && !(*di).empty(); ++col) {
      out->process_token((*di).front());
      (*di).pop();
    }

    //next table
    ++di;
    ++nci;
    if(di == data.end()) {
      out->process_line();
      di = data.begin();
      nci = num_columns.begin();
    }
  }

  table = 0;
  first_line = 1;
  more_lines = 1;
  data.clear();
}

void row_joiner::process()
{
  if(!out) throw runtime_error("row_joiner has no out");

  if(data.size()) process_lines();

  out->process_stream();
  table_name.clear();
  num_columns.clear();
  keys.clear();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// filter
////////////////////////////////////////////////////////////////////////////////////////////////

filter::filter() { init(); }
filter::filter(pass& out) { init(out); }

filter& filter::init()
{
  out = 0;
  keyword_limits.clear();
  keyword_limits.clear();
  first_row = 1;
  column = 0;
  column_limits.clear();
  return *this;
}

filter& filter::init(pass& out) { init(); return set_out(out); }
filter& filter::set_out(pass& out) { this->out = &out; return *this; }

filter& filter::add(bool regex, const char* keyword, double low_limit, double high_limit)
{
  if(regex) {
    const char* err; int err_off; pcre* p = pcre_compile(keyword, 0, &err, &err_off, 0);
    if(!p) throw runtime_error("filter can't compile regex");
    regex_limits.resize(regex_limits.size() + 1);
    regex_limits.back().regex = p;
    regex_limits.back().limits.low_limit = low_limit;
    regex_limits.back().limits.high_limit = high_limit;
  }
  else {
    limits_t& l = keyword_limits[keyword];
    l.low_limit = low_limit;
    l.high_limit = high_limit;
  }
  return *this;
}

void filter::process_token(const char* token)
{
  if(first_row) {
    if(!out) throw runtime_error("filter has no out");
    map<string, limits_t>::const_iterator i = keyword_limits.find(token);
    if(i != keyword_limits.end()) column_limits.push_back(pair<int, limits_t>(column, (*i).second));
    else {
      size_t len = strlen(token);
      for(vector<regex_limits_t>::const_iterator j = regex_limits.begin(); j != regex_limits.end(); ++j) {
        int ovector[30]; int rc = pcre_exec((*j).regex, 0, token, len, 0, 0, ovector, 30);
        if(rc >= 0) { column_limits.push_back(pair<int, limits_t>(column, (*j).limits)); break; }
        else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("filter match error");
      }
    }
    out->process_token(token);
  }
  else {
    if(cli == column_limits.end() || column != (*cli).first) out->process_token(token);
    else {
      char* next; double val = strtod(token, &next);
      if(next == token || val < (*cli).second.low_limit || val > (*cli).second.high_limit) out->process_token("");
      else { out->process_token(token); }
      ++cli;
    }
  }

  ++column;
}

void filter::process_line()
{
  if(!out) throw runtime_error("filter has no out");
  out->process_line();
  cli = column_limits.begin();
  first_row = 0;
  column = 0;
}

void filter::process_stream()
{
  if(!out) throw runtime_error("filter has no out");
  out->process_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// col_pruner
////////////////////////////////////////////////////////////////////////////////////////////////

col_pruner::col_pruner() : out(0), has_data(0) {}
col_pruner::col_pruner(pass& out) : has_data(0) { init(out); }

void col_pruner::init(pass& out)
{
  this->out = &out;
  first_row = 1;
  passthrough = 0;
  column = 0;
  delete[] has_data; has_data = 0;
  data.clear();
  data.set_default_cap(256 * 1024);
}

void col_pruner::process_token(const char* token)
{
  if(passthrough) { out->process_token(token); return; }

  if(first_row) {
    if(!out) throw runtime_error("col_pruner has no out");
  }
  else if(token[0]) {
    if(!(has_data[column / 32] & (1 << (column % 32)))) {
      has_data[column / 32] |= 1 << (column % 32);
      ++columns_with_data;
    }
  }

  data.push(token);
  ++column;
}

void col_pruner::process_line()
{
  if(passthrough) { out->process_line(); return; }

  if(first_row) {
    if(!out) throw runtime_error("col_pruner has no out");
    first_row = 0;
    num_columns = column;
    columns_with_data = 0;
    delete[] has_data; has_data = new uint32_t[(num_columns + 31) / 32];
  }
  else if(columns_with_data >= num_columns) {
    size_t c = 0;
    while(!data.empty()) {
      out->process_token(data.front());
      data.pop();
      if(++c >= num_columns) {
        out->process_line();
        c = 0;
      }
    }
    passthrough = 1;
    delete[] has_data; has_data = 0;
  }

  column = 0;
}

void col_pruner::process_stream()
{
  if(passthrough) { out->process_stream(); return; }

  if(!out) throw runtime_error("col_pruner has no out");

  size_t c = 0;
  while(!data.empty()) {
    if(has_data[c / 32] & (1 << (c % 32)))
      out->process_token(data.front());
    data.pop();
    if(++c >= num_columns) {
      out->process_line();
      c = 0;
    }
  }
  delete[] has_data; has_data = 0;
  out->process_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// combiner
////////////////////////////////////////////////////////////////////////////////////////////////

combiner::combiner() { init(); }
combiner::combiner(pass& out) { init(out); }

combiner::~combiner()
{
  for(vector<pair<pcre*, string> >::iterator i = pairs.begin(); i != pairs.end(); ++i) pcre_free((*i).first);
}

combiner& combiner::init()
{
  this->out = 0;
  for(vector<pair<pcre*, string> >::iterator i = pairs.begin(); i != pairs.end(); ++i) { pcre_free((*i).first); }
  pairs.clear();
  first_row = 1;
  column = 0;
  remap_indexes.clear();
  tokens.clear();
  return *this;
}

combiner& combiner::init(pass& out) { init(); return set_out(out); }
combiner& combiner::set_out(pass& out) { this->out = &out; return *this; }

combiner& combiner::add_pair(const char* from, const char* to)
{
  const char* err; int err_off; pcre* p = pcre_compile(from, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("combiner can't compile regex");
  pairs.resize(pairs.size() + 1);
  pairs.back().first = p;
  pairs.back().second = to;
  return *this;
}

void combiner::process_token(const char* token)
{
  if(first_row) {
    if(!out) throw runtime_error("combiner has no out");
    tokens.push_back(token);
  }
  else if(token[0]) {
    const int index = remap_indexes[column];
    if(!tokens[index].size()) tokens[index] = token;
    else throw runtime_error("multiple values for combiner");
  }

  ++column;
}

void combiner::process_line()
{
  if(first_row) {
    if(!out) throw runtime_error("combiner has no out");

    //create remap_indexes with gaps
    remap_indexes.resize(tokens.size());
    for(size_t i = 0; i < remap_indexes.size(); ++i) remap_indexes[i] = i;

    char* buf = new char[2048];
    char* end = buf + 2048;
    for(vector<pair<pcre*, string> >::iterator i = pairs.begin(); i != pairs.end(); ++i) {
      for(size_t j = 0; j < tokens.size(); ++j) {
        int ovector[30]; int rc = pcre_exec((*i).first, 0, tokens[j].c_str(), tokens[j].size(), 0, 0, ovector, 30);
        if(rc < 0) { if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("summarizer match error"); }
        else {
          char* next = buf;
          if(!rc) rc = 10;
          for(const char* rp = (*i).second.c_str(); 1; ++rp) {
            int br = 0;
            if(*rp == '\\' && isdigit(*(rp + 1)) && rc > (br = *(rp + 1) - '0')) {
              const char* cs = tokens[j].c_str() + ovector[br * 2];
              const char* ce = tokens[j].c_str() + ovector[br * 2 + 1];
              while(cs < ce) {
                if(next >= end) resize_buffer(buf, next, end);
                *next++ = *cs++;
              }
              ++rp;
            }
            else {
              if(next >= end) resize_buffer(buf, next, end);
              *next++ = *rp;
              if(!*rp) break;
            }
          }

          size_t k = 0;
          for(; k < tokens.size(); ++k) {
            if(j == k) continue;
            if(!tokens[k].compare(buf)) break;
          }
          if(k >= tokens.size()) {
            stringstream msg; msg << "can't find to (" << buf << ')';
            throw runtime_error(msg.str());
          }
          remap_indexes[j] = k;
          tokens[j] = buf;
        }
      }
    }
    delete[] buf;

    //compact out the gaps
    int max_out_index = 0;
    for(size_t i = 0; i < remap_indexes.size(); ++i)
      if(max_out_index < remap_indexes[i])
        max_out_index = remap_indexes[i];

    for(int i = 0; i <= max_out_index;) {
      vector<int>::iterator it = find(remap_indexes.begin(), remap_indexes.end(), i);
      if(it != remap_indexes.end()) { ++i; continue; }

      for(size_t j = 0; j < remap_indexes.size(); ++j) if(remap_indexes[j] > i) --remap_indexes[j];
      --max_out_index;
    }

    //print header
    for(int i = 0; i <= max_out_index; ++i) {
      vector<int>::iterator it = find(remap_indexes.begin(), remap_indexes.end(), i);
      if(it == remap_indexes.end()) throw runtime_error("wth");
      out->process_token(tokens[distance(remap_indexes.begin(), it)].c_str());
    }

    //setup for second line
    tokens.resize(max_out_index + 1);
    for(vector<string>::iterator i = tokens.begin(); i != tokens.end(); ++i) (*i).clear();
    first_row = 0;
  }
  else {
    for(vector<string>::iterator i = tokens.begin(); i != tokens.end(); ++i) {
      out->process_token((*i).c_str());
      (*i).clear();
    }
  }
  out->process_line();
  column = 0;
}

void combiner::process_stream()
{
  if(!out) throw runtime_error("combiner has no out");
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
      if(len + 1 > size_t(group_storage_end - group_storage_next)) {
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

  *group_storage_next++ = '\x04';

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
  out->process_token("F");
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
      double sstr = sum_of_sum_squared_over_count - total_sum_squared_over_total_count;
      double f = (sstr / (num_groups - 1)) / ((sst - sstr) / (total_count - num_groups));

      sprintf(group_tokens, "%f", total_sum / total_count); out->process_token(group_tokens);
      sprintf(group_tokens, "%f", sqrt(sst / (total_count - 1))); out->process_token(group_tokens);
      sprintf(group_tokens, "%f", f); out->process_token(group_tokens);
    }
    else {
      group_tokens[0] = '\0';
      out->process_token(group_tokens);
      out->process_token(group_tokens);
    }

    out->process_line();
  }

  for(data_t::iterator di = data.begin(); di != data.end(); ++di) delete[] *di;
  data.clear();

  out->process_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// writer
////////////////////////////////////////////////////////////////////////////////////////////////

void csv_writer_base::process_token(const char* token)
{
  if(!first_column) out->sputc(',');
  else {
    if(!out) throw runtime_error("writer has no out");
    first_column = 0;
  }

  for(const char* p = token; *p; ++p)
    out->sputc(*p);
}

void csv_writer_base::process_line()
{
  if(!out) throw runtime_error("writer has no out");
  out->sputc('\n');
  first_column = 1;
}


csv_writer::csv_writer() {}
csv_writer::csv_writer(streambuf* out) { init(out); }

void csv_writer::init(streambuf* out)
{
  this->out = out;
  if(!out) throw runtime_error("writer::init null out");
  first_column = 1;
}

void csv_writer::process_stream() {}

csv_file_writer::csv_file_writer() {}
csv_file_writer::csv_file_writer(const char* filename) { init(filename); }

void csv_file_writer::init(const char* filename)
{
  if(!out) out = new filebuf;
  else ((filebuf*)out)->close();

  if(!((filebuf*)out)->open(filename, ios_base::out | ios_base::trunc))
    throw runtime_error("file_writer can't open file");

  first_column = 1;
}

csv_file_writer::~csv_file_writer() { delete out; }

void csv_file_writer::process_stream() { ((filebuf*)out)->close(); }


////////////////////////////////////////////////////////////////////////////////////////////////
// driver
////////////////////////////////////////////////////////////////////////////////////////////////

void read_csv(streambuf* in, pass& out)
{
  if(!in) throw runtime_error("read_csv got a null in");

  bool first_line = 1;
  int num_keys = 0;
  int column = 0;
  bool blank = 1;
  char* buf = new char[4096];
  char* next = buf;
  char* end = buf + 4096;
  while(1) {
    int c = in->sbumpc();
    if(char_traits<char>::eof() == c) break;
    else if(c == '\n') {
      if(!blank) {
        if(next >= end) resize_buffer(buf, next, end);
        *next++ = '\0';
        out.process_token(buf);
        next = buf;
        ++column;
        if(first_line) { num_keys = column; first_line = 0; }
        else if(column != num_keys) throw runtime_error("didn't get same number of tokens as first line");
        out.process_line();
        column = 0;
        blank = 1;
      }
    }
    else if(c == ',') {
      if(next >= end) resize_buffer(buf, next, end);
      *next++ = '\0';
      out.process_token(buf);
      next = buf;
      ++column;
      blank = 0;
    }
    else {
      if(next >= end) resize_buffer(buf, next, end);
      *next++ = c;
      blank = 0;
    }
  }
  out.process_stream();
  delete[] buf;
}

void read_csv(const char* filename, pass& out)
{
  filebuf f;
  if(!f.open(filename, ios_base::in)) throw runtime_error("didn't open file for reading");
  read_csv(&f, out);
}

}
