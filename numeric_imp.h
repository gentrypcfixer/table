#ifndef numeric_imp_h_
#define numeric_imp_h_

#include <stdexcept>
#include <limits>
#include <math.h>
#include <stdio.h>

namespace table {

using namespace std;


////////////////////////////////////////////////////////////////////////////////////////////////
// unary_col_modifier
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename UnaryOperation> unary_col_modifier<UnaryOperation>::unary_col_modifier() { init(); }
template<typename UnaryOperation> unary_col_modifier<UnaryOperation>::unary_col_modifier(pass& out) { init(out); }

template<typename UnaryOperation> unary_col_modifier<UnaryOperation>& unary_col_modifier<UnaryOperation>::init()
{
  out = 0;
  insts.clear();
  first_row = 1;
  column = 0;
  column_insts.clear();
  return *this;
}

template<typename UnaryOperation> unary_col_modifier<UnaryOperation>& unary_col_modifier<UnaryOperation>::init(pass& out) { init(); return set_out(out); }
template<typename UnaryOperation> unary_col_modifier<UnaryOperation>& unary_col_modifier<UnaryOperation>::set_out(pass& out) { this->out = &out; return *this; }

template<typename UnaryOperation> unary_col_modifier<UnaryOperation>& unary_col_modifier<UnaryOperation>::add(const char* regex, const UnaryOperation& unary_op)
{
  const char* err; int err_off; pcre* p = pcre_compile(regex, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("unary_col_modifier can't compile regex");
  insts.resize(insts.size() + 1);
  insts.back().regex = p;
  insts.back().unary_op = unary_op;
  return *this;
}

template<typename UnaryOperation> void unary_col_modifier<UnaryOperation>::process_token(const char* token)
{
  if(first_row) {
    if(!out) throw runtime_error("unary_col_modifier has no out");
    UnaryOperation* p = 0;
    size_t len = strlen(token);
    for(typename vector<inst_t>::iterator i = insts.begin(); i != insts.end(); ++i) {
      int ovector[30]; int rc = pcre_exec((*i).regex, 0, token, len, 0, 0, ovector, 30);
      if(rc >= 0) { p = &(*i).unary_op; break; }
      else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("unary_col_modifier match error");
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

template<typename UnaryOperation> void unary_col_modifier<UnaryOperation>::process_line()
{
  if(!out) throw runtime_error("unary_col_modifier has no out");
  out->process_line();
  first_row = 0;
  column = 0;
}

template<typename UnaryOperation> void unary_col_modifier<UnaryOperation>::process_stream()
{
  if(!out) throw runtime_error("unary_col_modifier has no out");
  out->process_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// unary_col_adder
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename UnaryOperation> unary_col_adder<UnaryOperation>::unary_col_adder() : buf(0) { init(); }
template<typename UnaryOperation> unary_col_adder<UnaryOperation>::unary_col_adder(pass& out) : buf(0) { init(out); }

template<typename UnaryOperation> unary_col_adder<UnaryOperation>& unary_col_adder<UnaryOperation>::init()
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

template<typename UnaryOperation> unary_col_adder<UnaryOperation>& unary_col_adder<UnaryOperation>::init(pass& out) { init(); return set_out(out); }
template<typename UnaryOperation> unary_col_adder<UnaryOperation>& unary_col_adder<UnaryOperation>::set_out(pass& out) { this->out = &out; return *this; }

template<typename UnaryOperation> unary_col_adder<UnaryOperation>& unary_col_adder<UnaryOperation>::add(const char* regex, const char* new_key, const UnaryOperation& unary_op)
{
  const char* err; int err_off; pcre* p = pcre_compile(regex, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("unary_col_adder can't compile regex");
  insts.resize(insts.size() + 1);
  insts.back().regex = p;
  insts.back().new_key = new_key;
  insts.back().unary_op = unary_op;
  return *this;
}

template<typename UnaryOperation> void unary_col_adder<UnaryOperation>::process_token(const char* token)
{
  if(first_row) {
    if(!out) throw runtime_error("unary_col_adder has no out");
    size_t len = strlen(token);
    for(typename vector<inst_t>::iterator i = insts.begin(); i != insts.end(); ++i) {
      int ovector[30]; int rc = pcre_exec((*i).regex, 0, token, len, 0, 0, ovector, 30);
      if(rc < 0) { if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("unary_col_adder match error"); }
      else {
        columns.resize(columns.size() + 1);
        columns.back().col = column;
        columns.back().unary_op = &(*i).unary_op;
        char* next = buf;
        if(!rc) rc = 10;
        generate_substitution(token, (*i).new_key.c_str(), ovector, rc, buf, next, end);
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

template<typename UnaryOperation> void unary_col_adder<UnaryOperation>::process_line()
{
  if(first_row) {
    if(!out) throw runtime_error("unary_col_adder has no out");
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

template<typename UnaryOperation> void unary_col_adder<UnaryOperation>::process_stream()
{
  if(!out) throw runtime_error("unary_col_adder has no out");
  out->process_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// binary_col_modifier
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename BinaryOperation> binary_col_modifier<BinaryOperation>::binary_col_modifier() { init(); }
template<typename BinaryOperation> binary_col_modifier<BinaryOperation>::binary_col_modifier(pass& out) { init(out); }

template<typename BinaryOperation> binary_col_modifier<BinaryOperation>& binary_col_modifier<BinaryOperation>::init()
{
  out = 0;
  insts.clear();
  first_row = 1;
  column = 0;
  keys.clear();
  columns.clear();
  new_columns.clear();
  return *this;
}

template<typename BinaryOperation> binary_col_modifier<BinaryOperation>& binary_col_modifier<BinaryOperation>::init(pass& out) { init(); return set_out(out); }
template<typename BinaryOperation> binary_col_modifier<BinaryOperation>& binary_col_modifier<BinaryOperation>::set_out(pass& out) { this->out = &out; return *this; }

template<typename BinaryOperation> binary_col_modifier<BinaryOperation>& binary_col_modifier<BinaryOperation>::add(const char* regex, const char* other_key, const BinaryOperation& binary_op)
{
  const char* err; int err_off; pcre* p = pcre_compile(regex, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("binary_col_modifier can't compile regex");
  insts.resize(insts.size() + 1);
  insts.back().regex = p;
  insts.back().other_key = other_key;
  insts.back().binary_op = binary_op;
  return *this;
}

template<typename BinaryOperation> void binary_col_modifier<BinaryOperation>::process_token(const char* token)
{
  if(first_row) {
    if(!out) throw runtime_error("binary_col_modifier has no out");
    keys.push_back(token);
  }
  else {
    if(ci == columns.end() || (*ci).col != column) out->process_token(token);
    else {
      char* next; (*ci).val = strtod(token, &next);
      if(next == token) (*ci).val = numeric_limits<double>::quiet_NaN();

      if((*ci).passthrough) out->process_token(token);

      ++ci;
    }
  }

  ++column;
}

template<typename BinaryOperation> void binary_col_modifier<BinaryOperation>::process_line()
{
  if(first_row) {
    if(!out) throw runtime_error("binary_col_modifier has no out");

    char* buf = new char[2048];
    char* end = buf + 2048;
    map<size_t, col_info_t> cols;
    map<size_t, new_col_info_t> new_cols;

    for(column = 0; column < keys.size(); ++column) {
      for(typename vector<inst_t>::iterator ii = insts.begin(); ii != insts.end(); ++ii) {
        int ovector[30]; int rc = pcre_exec((*ii).regex, 0, keys[column].c_str(), keys[column].size(), 0, 0, ovector, 30);
        if(rc < 0){ if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("binary_col_modifier match error"); }
        else {
          char* next = buf;
          if(!rc) rc = 10;
          generate_substitution(keys[column].c_str(), (*ii).other_key.c_str(), ovector, rc, buf, next, end);
          vector<string>::iterator ki = find(keys.begin(), keys.end(), buf);
          if(ki != keys.end()) { 
            size_t other_col = distance(keys.begin(), ki);
            cols[column].passthrough = 0;
            if(cols.end() == cols.find(other_col)) cols[other_col].passthrough = 1;
            new_col_info_t& nci = new_cols[column];
            nci.other_col = other_col;
            nci.binary_op = &(*ii).binary_op;
            break;
          }
        }
      }
    }

    for(column = 0; column < keys.size(); ++column) {
      typename map<size_t, col_info_t>::iterator i = cols.find(column);
      if(i == cols.end() || (*i).second.passthrough)
        out->process_token(keys[column].c_str());
    }

    for(typename map<size_t, col_info_t>::iterator i = cols.begin(); i != cols.end(); ++i) {
      (*i).second.index = columns.size();
      col_t c;
      c.col = (*i).first;
      c.passthrough = (*i).second.passthrough;
      columns.push_back(c);
    }

    for(typename map<size_t, new_col_info_t>::iterator i = new_cols.begin(); i != new_cols.end(); ++i) {
      new_col_t c;
      c.col_index = cols[(*i).first].index;
      c.other_col_index = cols[(*i).second.other_col].index;
      c.binary_op = (*i).second.binary_op;
      new_columns.push_back(c);
      out->process_token(keys[(*i).first].c_str());
    }

    delete[] buf;
    first_row = 0;
    keys.clear();
  }
  else {
    for(typename vector<new_col_t>::iterator i = new_columns.begin(); i != new_columns.end(); ++i) {
      double val = (*(*i).binary_op)(columns[(*i).col_index].val, columns[(*i).other_col_index].val);
      char buf[32];
      sprintf(buf, "%f", val);
      out->process_token(buf);
    }
  }
  out->process_line();
  column = 0;
  ci = columns.begin();
}

template<typename BinaryOperation> void binary_col_modifier<BinaryOperation>::process_stream()
{
  if(!out) throw runtime_error("binary_col_modifier has no out");
  out->process_stream();
}


}

#endif
