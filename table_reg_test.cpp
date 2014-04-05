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

void generate_data(pass& out, size_t num_columns, size_t num_lines)
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


////////////////////////////////////////////////////////////////////////////////////////////////
// simple_validator
////////////////////////////////////////////////////////////////////////////////////////////////

class simple_validater : public pass {
  const char** expected;
  size_t column;
  size_t line;

  public:
  simple_validater(const char** expected) : expected(expected), column(0), line(0) {}

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
  "L0_C0", "L0,C1", "keyword", "data",  0,
  "L1_C0", "L1_C1", "L0_C3",   "L1_C3", 0,
  "L2_C0", "L2_C1", "L2_C3",   "L2_C3", 0,
  0
};

int validate_stacker()
{
  int ret_val = 0;

  try {
    simple_validater v(stacker_expect);

    stacker st(v, ST_STACK);
    st.add_action(0, "L0_C1", ST_LEAVE);
    st.add_action(0, "L0_C3", ST_REMOVE);

    generate_data(st, 4, 2);

    st.init(v, ST_LEAVE);
    st.add_action(0, "L0_C2", ST_REMOVE);
    st.add_action(0, "L0_C3", ST_STACK);

    generate_data(st, 4, 3);
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

  return ret_val;
}

