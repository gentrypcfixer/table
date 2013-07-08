#include <iostream>
#include <stdexcept>
#include "table.h"

using namespace std;
using namespace table;


////////////////////////////////////////////////////////////////////////////////////////////////
// calc
////////////////////////////////////////////////////////////////////////////////////////////////

class calculator : public pass {
  pass* out;
  bool first_row;
  vector<int> columns;

  int column;
  bool blank;
  vector<double> values;

public:
  calculator(pass& out) {
    this->out = &out;
    first_row = 1;
    columns.clear();
    columns.resize(2, -1);
    column = 0;
    blank = 0;
    values.resize(2);
  }

  void process_token(const char* token);
  void process_line();
  void process_stream() { out->process_stream(); }
};

void calculator::process_token(const char* token)
{
  if(first_row) {
    if(token == "base") columns[0] = column;
    else if(token == "comp") columns[1] = column;
  }
  else {
    vector<int>::iterator i = find(columns.begin(), columns.end(), column);
    if(i != columns.end()) {
      istringstream ss(token); ss >> values[distance(columns.begin(), i)]; if(!ss) blank = 1;
    }
  }
  out->process_token(token);

  ++column;
}

void calculator::process_line()
{
  if(first_row) {
    for(vector<int>::const_iterator i = columns.begin(); i != columns.end(); ++i)
      if((*i) < 0) throw runtime_error("calculator couldn't find a column");
    out->process_token("calc");
    first_row = 0;
  }
  else {
    if(blank) out->process_token("");
    else {
      stringstream ss; ss << values[1] - values[0];
      out->process_token(ss.str().c_str());
    }
  }
  out->process_line();
  column = 0;
  blank = 0;
}


////////////////////////////////////////////////////////////////////////////////////////////////
// main
////////////////////////////////////////////////////////////////////////////////////////////////

int main(const unsigned int argc, char const * const argv[])
{
  int ret_val = 0;

  try {
    writer w(cout.rdbuf());

    differ d1(w, "data 7BEO", "data RA07B", "delta V5400");
    differ d2(d1, "data RA07B", "data J46", "delta C1D");

    splitter sp(d2, SP_REMOVE);
    sp.add_action(0, "LOT", SP_GROUP);
    sp.add_action(0, "WAFER", SP_GROUP);
    sp.add_action(0, "ROW", SP_GROUP);
    sp.add_action(0, "COL", SP_GROUP);
    sp.add_action(0, "keyword", SP_GROUP);
    sp.add_action(0, "Group", SP_SPLIT_BY);
    sp.add_action(0, "data", SP_SPLIT);

    stacker st(sp, ST_STACK);
    st.add_action(0, "LOT", ST_LEAVE);
    st.add_action(0, "WAFER", ST_LEAVE);
    st.add_action(0, "ROW", ST_LEAVE);
    st.add_action(0, "COL", ST_LEAVE);
    st.add_action(0, "WAFSIZE", ST_REMOVE);
    st.add_action(0, "Process_id", ST_LEAVE);
    st.add_action(0, "Fail_bin", ST_LEAVE);
    st.add_action(0, "Error_bin", ST_LEAVE);
    st.add_action(0, "Group", ST_LEAVE);

    calculator ca(sp);

    combiner c(ca);
    c.add_pair("BIN_O_FTR_(.*)", "BIN_OO_\\1");

    read_csv(cin.rdbuf(), c);
  }
  catch(exception& e) { cerr << "Exception: " << e.what() << endl; }
  catch(...) { cerr << "Unknown Exception" << endl; }

  return ret_val;
}

