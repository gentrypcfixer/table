#include <iostream>
#include <sstream>
#include <stdexcept>
#include <algorithm>
#include "table.h"

using namespace std;
using namespace table;


////////////////////////////////////////////////////////////////////////////////////////////////
// data_generators
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename out_t> void generate_data(out_t& out, size_t num_columns, size_t num_lines)
{
  char buf[2048];

  for(size_t line = 0; line < num_lines; ++line) {
    char* next = buf;
    size_t base_len = sprintf(next, "L%zu_C", line);
    next += base_len;

    for(size_t column = 0; column < num_columns; ++column) {
      size_t len = sprintf(next, "%zu", column) + base_len;
      out.process_token(buf, len);
    }

    out.process_line();
  }

  out.process_stream();
};

template<typename out_t> void generate_numeric_data(out_t& out, size_t num_columns, size_t num_lines)
{
  char buf[2048];
  size_t count = 0;

  for(size_t line = 0; line < num_lines; ++line) {
    for(size_t column = 0; column < num_columns; ++column) {
      size_t len;
      if(!line) len = sprintf(buf, "C%zu", column);
      else len = sprintf(buf, "%zu", count++);
      out.process_token(buf, len);
    }

    out.process_line();
  }

  out.process_stream();
};


////////////////////////////////////////////////////////////////////////////////////////////////
// feed_data
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename out_t> void feed_data(out_t& out, const char** data)
{
  bool f = 0;
  const char** p = data;

  while(1) {
    if(*p) {
      size_t len = strlen(*p);
      out.process_token(*p, len);
      ++p;
      f = 1;
    }
    else if(f) { out.process_line(); ++p; f = 0; }
    else break;
  }

  out.process_stream();
}

////////////////////////////////////////////////////////////////////////////////////////////////
// simple_validator
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t> class basic_simple_validater_t : public input_base_t
{
  const char** expected;
  size_t column;
  size_t line;

  public:
  basic_simple_validater_t() : expected(0), column(0), line(0) {}

  void reinit(int more_passes = 0) { reinit_state(); }
  void reinit_state(int more_passes = 0) { column = 0; line = 0; }
  void set_expected(const char** expected) { this->expected = expected; }

  void process_token(const char* token, size_t len) {
    if(!*expected) {
      stringstream msg;
      msg << "line " << line << ". is too long: ";
      if(token) msg << " got \"" << token << '\"';
      throw runtime_error(msg.str());
    }
    else if(strcmp(token, *expected)) {
      stringstream msg;
      if(token) msg << "got \"" << token << "\" ";
      msg << "expected \"" << *expected << "\" on [" << line << ':' << column << ']';
      throw runtime_error(msg.str());
    }

    ++expected;
    ++column;
  }

  void process_token(double token) { char buf[32]; size_t len = dtostr(token, buf); process_token(buf, len); }

  void process_line() {
    if(*expected) {
      stringstream msg; msg << "line " << line << " is too short";
      throw runtime_error(msg.str());
    }
    ++expected;
    column = 0;
    ++line;
  }

  void process_stream() {
    if(column && line) {
      stringstream msg; msg << "process_stream called after process_token";
      throw runtime_error(msg.str());
    }
    else if(*expected) {
      stringstream msg; msg << "got " << line << " lines, but expected more";
      throw runtime_error(msg.str());
    }
  }
};

class simple_validater : public basic_simple_validater_t<empty_class_t> {};
class dynamic_simple_validater : public basic_simple_validater_t<dynamic_pass_t> {};

////////////////////////////////////////////////////////////////////////////////////////////////
// stacker
////////////////////////////////////////////////////////////////////////////////////////////////

const char* stacker_expect[] = {
  "L0_C1", "keyword", "data",  0,
  "L1_C1", "L0_C0",   "L1_C0", 0,
  "L1_C1", "L0_C2",   "L1_C2", 0,
  0
};

const char* stacker_expect2[] = {
  "L0_C0", "L0_C1", "keyword", "data",  0,
  "L1_C0", "L1_C1", "L0_C3",   "L1_C3", 0,
  "L2_C0", "L2_C1", "L0_C3",   "L2_C3", 0,
  0
};

const char* stacker_expect3[] = {
  "C0", "C1", "keyword", "data", 0,
  "0",  "1",  "C3",      "3",    0,
  "4",  "5",  "C3",      "7",    0,
  0
};

int validate_stacker()
{
  int ret_val = 0;

  try {

    stacker<simple_validater> st;
    st.set_default_action(ST_STACK);
    st.add_action(0, "L0_C1", ST_LEAVE);
    st.add_action(0, "L0_C3", ST_REMOVE);
    simple_validater& v = st.get_out();
    v.set_expected(stacker_expect);
    generate_data(st, 4, 2);


    st.reinit(-1);
    st.set_default_action(ST_LEAVE);
    st.add_action(0, "L0_C2", ST_REMOVE);
    st.add_action(0, "L0_C3", ST_STACK);
    v.set_expected(stacker_expect2);
    generate_data(st, 4, 3);


    st.reinit(-1);
    st.set_default_action(ST_LEAVE);
    st.add_action(0, "C2", ST_REMOVE);
    st.add_action(0, "C3", ST_STACK);
    v.set_expected(stacker_expect3);
    generate_numeric_data(st, 4, 3);
  }
  catch(exception& e) { cerr << __func__ << " exception: " << e.what() << endl; ret_val = 1; }
  catch(...) { cerr << __func__ << " unknown Exception" << endl; ret_val = 1; }
  
  return ret_val;
}


////////////////////////////////////////////////////////////////////////////////////////////////
// splitter
////////////////////////////////////////////////////////////////////////////////////////////////

const char* splitter_input[] = {
  "C0", "C1", "C2", "C3", "C4", "C5", 0,
  "0",  "2",  "10", "12", "20", "28", 0,
  "0",  "2",  "10", "13", "21", "29", 0,
  "0",  "3",  "11", "12", "22", "30", 0,
  "0",  "3",  "11", "13", "23", "31", 0,
  "1",  "2",  "10", "12", "24", "32", 0,
  "1",  "2",  "10", "13", "25", "33", 0,
  "1",  "3",  "11", "12", "26", "34", 0,
  "1",  "3",  "11", "13", "27", "35", 0,
  0
};

const char* splitter_expect[] = {
  "C0", "C1", "C4 10 12", "C5 10 12", "C4 10 13",  "C5 10 13", "C4 11 12", "C5 11 12", "C4 11 13", "C5 11 13", 0,
  "0",  "2",  "20",       "28",       "21",        "29",       "",         "",         "",         "",         0,
  "0",  "3",  "",         "",         "",          "",         "22",       "30",       "23",       "31",       0,
  "1",  "2",  "24",       "32",       "25",        "33",       "",         "",         "",         "",         0,
  "1",  "3",  "",         "",         "",          "",         "26",       "34",       "27",       "35",       0,
  0
};

int validate_splitter()
{
  int ret_val = 0;

  try {
    splitter<simple_validater> s;
    s.set_default_action(SP_REMOVE);
    s.add_action(0, "C0", SP_GROUP);
    s.add_action(0, "C1", SP_GROUP);
    s.add_action(0, "C2", SP_SPLIT_BY);
    s.add_action(0, "C3", SP_SPLIT_BY);
    s.add_action(0, "C4", SP_SPLIT);
    s.add_action(0, "C5", SP_SPLIT);
    simple_validater& v = s.get_out();
    v.set_expected(splitter_expect);
    feed_data(s, splitter_input);
  }
  catch(exception& e) { cerr << __func__ << " exception: " << e.what() << endl; ret_val = 1; }
  catch(...) { cerr << __func__ << " unknown Exception" << endl; ret_val = 1; }
  
  return ret_val;
}


////////////////////////////////////////////////////////////////////////////////////////////////
// sorter
////////////////////////////////////////////////////////////////////////////////////////////////

const char* sorter_input[] = {
  "C0", "C1", "C2",  0,
  "1",  "1",  "2",   0,
  "1",  "4",  "5",   0,
  "0",  "7",  "8",   0,
  "0",  "10", "11",  0,
  0
};

const char* sorter_expect[] = {
  "C0", "C2",  "C1", 0,
  "0",  "8",   "7",  0,
  "0",  "11",  "10", 0,
  "1",  "5",   "4",  0,
  "1",  "2",   "1",  0,
  0
};

int validate_sorter()
{
  int ret_val = 0;

  try {
    sorter<simple_validater> so;
    so.add_sort("C0", 1);
    so.add_sort("C2", 0);
    simple_validater& v = so.get_out();
    v.set_expected(sorter_expect);
    feed_data(so, sorter_input);
  }
  catch(exception& e) { cerr << __func__ << " exception: " << e.what() << endl; ret_val = 1; }
  catch(...) { cerr << __func__ << " unknown Exception" << endl; ret_val = 1; }
  
  return ret_val;
}


////////////////////////////////////////////////////////////////////////////////////////////////
// unary_col_adder
////////////////////////////////////////////////////////////////////////////////////////////////

const char* uca_dd_expect[] = {
  "tripple", 0,
  "0",       0,
  "3",       0,
  0
};

double tripple(double a) { return a * 3; }

const char* uca_ss_expect[] = {
  "ss_hash", "L0_C0", 0,
  "ODD",     "L1_C0", 0,
  "EVEN",    "L2_C0", 0,
  0
};

c_str_and_len_t uca_ss_hash(c_str_and_len_t a)
{
  c_str_and_len_t res;

  const char* pa = a.c_str;
  uint8_t sum = 0;
  for(size_t i = 0; i < a.len; ++i, ++pa) {
    sum = (sum ^ a.c_str[i]) * 59;
  }

  if(sum & 2) { res.c_str = "ODD"; res.len = 3; }
  else { res.c_str = "EVEN"; res.len = 4; }

  return res;
}

const char* uca_ds_expect[] = {
  "cat",  "C0", 0,
  "EVEN", "0",  0,
  "ODD",  "1",  0,
  0
};

c_str_and_len_t uca_cat(double a)
{
  c_str_and_len_t res;
  if(lround(a) & 1) { res.c_str = "ODD"; res.len = 3; }
  else { res.c_str = "EVEN"; res.len = 4; }
  return res;
}

const char* uca_sd_expect[] = {
  "sd_hash", 0,
  "243",     0,
  "44",      0,
  0
};

double uca_sd_hash(c_str_and_len_t a)
{
  const char* pa = a.c_str;
  uint8_t sum = 0;
  for(size_t i = 0; i < a.len; ++i, ++pa) {
    sum = (sum ^ a.c_str[i]) * 59;
  }
  return sum;
}

const char* uca_sub_expect[] = {
  "L0_C1", "L0_C0", 0,
  "L1_C1", "L1_C0", 0,
  "L2_C1", "L2_C0", 0,
  0
};

int validate_unary_col_adder()
{
  int ret_val = 0;

  try {
    unary_col_adder<simple_validater> a;
    a.add("^C0$", "tripple", tripple, 1);
    a.get_out().set_expected(uca_dd_expect);
    generate_numeric_data(a, 1, 3);

    unary_c_str_col_adder<simple_validater> sa;
    sa.add("^L0_C0$", "ss_hash", uca_ss_hash);
    sa.get_out().set_expected(uca_ss_expect);
    generate_data(sa, 1, 3);

    unary_double_c_str_col_adder<simple_validater> dsa;
    dsa.add("^C0$", "cat", uca_cat);
    dsa.get_out().set_expected(uca_ds_expect);
    generate_numeric_data(dsa, 1, 3);

    unary_c_str_double_col_adder<simple_validater> sda;
    sda.add("^L0_C0$", "sd_hash", uca_sd_hash, 1);
    sda.get_out().set_expected(uca_sd_expect);
    generate_data(sda, 1, 3);

    substitute_col_adder<simple_validater> suba;
    substituter sub("L(\\d+)_C0", "L\\1_C1");
    suba.add("^L0_C0$", "L0_C1", sub);
    suba.get_out().set_expected(uca_sub_expect);
    generate_data(suba, 1, 3);
  }
  catch(exception& e) { cerr << __func__ << " exception: " << e.what() << endl; ret_val = 1; }
  catch(...) { cerr << __func__ << " unknown Exception" << endl; ret_val = 1; }
  
  return ret_val;
}


////////////////////////////////////////////////////////////////////////////////////////////////
// binary_col_adder
////////////////////////////////////////////////////////////////////////////////////////////////

double sum(double a, double b) { return a + b; }
double mult(double a, double b) { return a * b; }

const char* bca_dd_expect[] = {
  "C0", "C0sum", "mult",  0,
  "0",  "1",     "0",     0,
  "2",  "5",     "6",     0,
  0
};

const char* bca_ss_expect[] = {
  "L0_C0", "ss_hash", 0,
  "L1_C0", "EVEN",    0,
  "L2_C0", "ODD",     0,
  0
};

c_str_and_len_t bca_ss_hash(c_str_and_len_t a, c_str_and_len_t b)
{
  c_str_and_len_t res;

  const char* pa = a.c_str;
  const char* pb = b.c_str;
  size_t len = min(a.len, b.len);
  uint8_t sum = 0;
  for(size_t i = 0; i < len; ++i, ++pa, ++pb) {
    sum = (sum ^ a.c_str[i]) * 59;
    sum = (sum ^ b.c_str[i]) * 59;
  }

  if(sum & 2) { res.c_str = "ODD"; res.len = 3; }
  else { res.c_str = "EVEN"; res.len = 4; }

  return res;
}

const char* bca_ds_expect[] = {
  "C0", "C1", "cat", 0,
  "0",  "1",  "EVEN",    0,
  "2",  "3",  "ODD",     0,
  0
};

c_str_and_len_t bca_cat(double a, double b)
{
  c_str_and_len_t res;
  if(lround(a + b) & 4) { res.c_str = "ODD"; res.len = 3; }
  else { res.c_str = "EVEN"; res.len = 4; }
  return res;
}

const char* bca_sd_expect[] = {
  "L0_C0", "L0_C1", "sd_hash", 0,
  "L1_C0", "L1_C1", "213",    0,
  "L2_C0", "L2_C1", "211",     0,
  0
};

double bca_sd_hash(c_str_and_len_t a, c_str_and_len_t b)
{
  const char* pa = a.c_str;
  const char* pb = b.c_str;
  size_t len = min(a.len, b.len);
  uint8_t sum = 0;
  for(size_t i = 0; i < len; ++i, ++pa, ++pb) {
    sum = (sum ^ a.c_str[i]) * 59;
    sum = (sum ^ b.c_str[i]) * 59;
  }
  return sum;
}

int validate_binary_col_adder()
{
  int ret_val = 0;

  try {
    binary_col_adder<simple_validater> a;
    a.add("^C0$", "C1", "\\0sum", sum);
    a.add("^C1$", "C0", "mult", mult, 1, 0);
    a.get_out().set_expected(bca_dd_expect);
    generate_numeric_data(a, 2, 3);

    binary_c_str_col_adder<simple_validater> sa;
    sa.add("^L0_C0$", "L0_C1", "ss_hash", bca_ss_hash, 0, 1);
    sa.get_out().set_expected(bca_ss_expect);
    generate_data(sa, 2, 3);

    binary_double_c_str_col_adder<simple_validater> dsa;
    dsa.add("^C0$", "C1", "cat", bca_cat);
    dsa.get_out().set_expected(bca_ds_expect);
    generate_numeric_data(dsa, 2, 3);

    binary_c_str_double_col_adder<simple_validater> sda;
    sda.add("^L0_C0$", "L0_C1", "sd_hash", bca_sd_hash);
    sda.get_out().set_expected(bca_sd_expect);
    generate_data(sda, 2, 3);
  }
  catch(exception& e) { cerr << __func__ << " exception: " << e.what() << endl; ret_val = 1; }
  catch(...) { cerr << __func__ << " unknown Exception" << endl; ret_val = 1; }
  
  return ret_val;
}


////////////////////////////////////////////////////////////////////////////////////////////////
// summarizer
////////////////////////////////////////////////////////////////////////////////////////////////

const char* summarizer_input[] = {
  "C0", "C1", "C2",  0,
  "0",  "1",  "2",   0,
  "0",  "4",  "5",   0,
  "0",  "4",  "7",   0,
  "1",  "7",  "8",   0,
  "1",  "7",  "",    0,
  "1",  "10", "11",  0,
  0
};

const char* summarizer_expect[] = {
  "C0", "C1", "COUNT(C1)", "MISSING(C2)", "COUNT(C2)", "MAX(C2)",  0,
  "0",  "1",          "1",           "0",         "1",       "2",  0,
  "0",  "4",          "2",           "0",         "2",       "7",  0,
  "1",  "7",          "2",           "1",         "1",       "8",  0,
  "1",  "10",         "1",           "0",         "1",      "11",  0,
  0
};

int validate_summarizer()
{
  int ret_val = 0;

  try {
    summarizer<simple_validater> su;
    su.add_group("^C0$", 1);
    su.add_group("^C1$");
    su.add_data("^C1$", SUM_COUNT);
    su.add_data("^C2$", SUM_MISSING | SUM_COUNT | SUM_MAX);
    su.get_out().set_expected(summarizer_expect);
    feed_data(su, summarizer_input);
  }
  catch(exception& e) { cerr << __func__ << " exception: " << e.what() << endl; ret_val = 1; }
  catch(...) { cerr << __func__ << " unknown Exception" << endl; ret_val = 1; }
  
  return ret_val;
}


////////////////////////////////////////////////////////////////////////////////////////////////
// range_stacker
////////////////////////////////////////////////////////////////////////////////////////////////

const char* range_stacker_expect[] = {
  "C0", "X", "Y", 0,
  "0",  "1", "3", 0,
  "0",  "1", "4", 0,
  "0",  "2", "3", 0,
  "0",  "2", "4", 0,
  "5",  "6", "8", 0,
  "5",  "6", "9", 0,
  "5",  "7", "8", 0,
  "5",  "7", "9", 0,
  0
};

int validate_range_stacker()
{
  int ret_val = 0;

  try {
    range_stacker<simple_validater> s;
    s.add("C1", "C2", "X");
    s.add("C3", "C4", "Y");
    s.get_out().set_expected(range_stacker_expect);
    generate_numeric_data(s, 5, 3);
  }
  catch(exception& e) { cerr << __func__ << " exception: " << e.what() << endl; ret_val = 1; }
  catch(...) { cerr << __func__ << " unknown Exception" << endl; ret_val = 1; }
  
  return ret_val;
}


////////////////////////////////////////////////////////////////////////////////////////////////
// main
////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char * argv[])
{
  int ret_val = validate_stacker();
  validate_splitter();
  validate_sorter();
  validate_unary_col_adder();
  validate_binary_col_adder();
  validate_summarizer();
  validate_range_stacker();

  return ret_val;
}

