#ifndef numeric_imp_h_
#define numeric_imp_h_

#include <stdexcept>
#include <limits>
#include <math.h>
#include <stdio.h>

namespace table {

using namespace std;


////////////////////////////////////////////////////////////////////////////////////////////////
// unary_modifier
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename UnaryOperation> unary_modifier<UnaryOperation>::unary_modifier() { init(); }
template<typename UnaryOperation> unary_modifier<UnaryOperation>::unary_modifier(pass& out) { init(out); }

template<typename UnaryOperation> unary_modifier<UnaryOperation>& unary_modifier<UnaryOperation>::init()
{
  out = 0;
  insts.clear();
  first_row = 1;
  column = 0;
  column_insts.clear();
  return *this;
}

template<typename UnaryOperation> unary_modifier<UnaryOperation>& unary_modifier<UnaryOperation>::init(pass& out) { init(); return set_out(out); }
template<typename UnaryOperation> unary_modifier<UnaryOperation>& unary_modifier<UnaryOperation>::set_out(pass& out) { this->out = &out; return *this; }

template<typename UnaryOperation> unary_modifier<UnaryOperation>& unary_modifier<UnaryOperation>::add(const char* regex, const UnaryOperation& unary_op)
{
  const char* err; int err_off; pcre* p = pcre_compile(regex, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("unary_modifier can't compile regex");
  insts.resize(insts.size() + 1);
  insts.back().regex = p;
  insts.back().unary_op = unary_op;
  return *this;
}

template<typename UnaryOperation> void unary_modifier<UnaryOperation>::process_token(const char* token)
{
  if(first_row) {
    if(!out) throw runtime_error("unary_modifier has no out");
    UnaryOperation* p = 0;
    size_t len = strlen(token);
    for(typename vector<inst_t>::iterator i = insts.begin(); i != insts.end(); ++i) {
      int ovector[30]; int rc = pcre_exec((*i).regex, 0, token, len, 0, 0, ovector, 30);
      if(rc >= 0) { p = &(*i).unary_op; break; }
      else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("unary_modifier match error");
    }
    column_insts.push_back(p);
    out->process_token(token);
  }
  else {
    if(!column_insts[column]) out->process_token(token);
    else {
      char* next; double val = strtod(token, &next);
      if(next == token) val = numeric_limits<double>::quiet_NaN();
      val = (*column_insts[column])(val);

      if(isnan(val)) out->process_token("");
      else {
        char buf[32];
        sprintf(buf, "%f", val);
        out->process_token(buf);
      }
    }
  }

  ++column;
}

template<typename UnaryOperation> void unary_modifier<UnaryOperation>::process_line()
{
  if(!out) throw runtime_error("unary_modifier has no out");
  out->process_line();
  first_row = 0;
  column = 0;
}

template<typename UnaryOperation> void unary_modifier<UnaryOperation>::process_stream()
{
  if(!out) throw runtime_error("unary_modifier has no out");
  out->process_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// unary_adder
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename UnaryOperation> unary_adder<UnaryOperation>::unary_adder() : buf(0) { init(); }
template<typename UnaryOperation> unary_adder<UnaryOperation>::unary_adder(pass& out) : buf(0) { init(out); }

template<typename UnaryOperation> unary_adder<UnaryOperation>& unary_adder<UnaryOperation>::init()
{
  out = 0;
  insts.clear();
  first_row = 1;
  column = 0;
  delete[] buf; buf = new char[2048];
  end = buf + 2048;
  columns.clear();
  return *this;
}

template<typename UnaryOperation> unary_adder<UnaryOperation>& unary_adder<UnaryOperation>::init(pass& out) { init(); return set_out(out); }
template<typename UnaryOperation> unary_adder<UnaryOperation>& unary_adder<UnaryOperation>::set_out(pass& out) { this->out = &out; return *this; }

template<typename UnaryOperation> unary_adder<UnaryOperation>& unary_adder<UnaryOperation>::add(const char* regex, const char* new_key, const UnaryOperation& unary_op)
{
  const char* err; int err_off; pcre* p = pcre_compile(regex, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("unary_adder can't compile regex");
  insts.resize(insts.size() + 1);
  insts.back().regex = p;
  insts.back().new_key = new_key;
  insts.back().unary_op = unary_op;
  return *this;
}

template<typename UnaryOperation> void unary_adder<UnaryOperation>::process_token(const char* token)
{
  if(first_row) {
    if(!out) throw runtime_error("unary_adder has no out");
    size_t len = strlen(token);
    for(typename vector<inst_t>::iterator i = insts.begin(); i != insts.end(); ++i) {
      int ovector[30]; int rc = pcre_exec((*i).regex, 0, token, len, 0, 0, ovector, 30);
      if(rc < 0) { if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("unary_adder match error"); }
      else {
        columns.resize(columns.size() + 1);
        columns.back().col = column;
        columns.back().unary_op = &(*i).unary_op;
        char* next = buf;
        if(!rc) rc = 10;
        for(const char* tp = (*i).new_key.c_str(); 1; ++tp) {
          int br = 10;
          if(*tp == '\\' && isdigit(*(tp + 1))) br = *(tp + 1) - '0';
          if(br < rc) {
            const char* cs = token + ovector[br * 2];
            const char* ce = token + ovector[br * 2 + 1];
            while(cs < ce) {
              if(next >= end) resize_buffer(buf, next, end);
              *next++ = *cs++;
            }
            ++tp;
          }
          else {
            if(next >= end) resize_buffer(buf, next, end);
            *next++ = *tp;
            if(!*tp) break;
          }
        }
        columns.back().new_key = buf;
      }
    }
    out->process_token(token);
  }
  else {
    if(ci != columns.end() && column == (*ci).col) {
      char* next; double val = strtod(token, &next);
      if(next == token) val = numeric_limits<double>::quiet_NaN();
      do { (*ci++).val = val; } while(ci != columns.end() && column == (*ci).col);
    }
    out->process_token(token);
  }

  ++column;
}

template<typename UnaryOperation> void unary_adder<UnaryOperation>::process_line()
{
  if(first_row) {
    if(!out) throw runtime_error("unary_adder has no out");
    first_row = 0;
    for(typename vector<col_t>::iterator i = columns.begin(); i != columns.end(); ++i) {
      out->process_token((*i).new_key.c_str());
      (*i).new_key.clear();
    }
  }
  else {
    for(typename vector<col_t>::iterator i = columns.begin(); i != columns.end(); ++i) {
      double val = (*(*i).unary_op)((*i).val);

      if(isnan(val)) out->process_token("");
      else {
        char buf[32];
        sprintf(buf, "%f", val);
        out->process_token(buf);
      }
    }
  }
  out->process_line();
  column = 0;
  ci = columns.begin();
}

template<typename UnaryOperation> void unary_adder<UnaryOperation>::process_stream()
{
  if(!out) throw runtime_error("unary_adder has no out");
  out->process_stream();
}


}

#endif
