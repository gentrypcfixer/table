#include <iostream>
#include <sstream>
#include <stdexcept>
#include <algorithm>
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
  vector<string> values;

public:
  calculator(pass& out) {
    this->out = &out;
    first_row = 1;
    columns.clear();
    columns.resize(4, -1);
    column = 0;
    values.resize(4);
  }

  void process_token(const char* token);
  void process_line();
  void process_stream() { out->process_stream(); }
};

void calculator::process_token(const char* token)
{
  if(first_row) {
    if(!strcmp(token, "EVS_VWVT_BLK(1)")) columns[0] = column;
    else if(!strcmp(token, "EVS_VWVT_BLK(2)")) columns[1] = column;
    else if(!strcmp(token, "EVS_INTRINSIC_EDGE(1)")) columns[2] = column;
    else if(!strcmp(token, "EVS_INTRINSIC_EDGE(2)")) columns[3] = column;
  }
  else {
    vector<int>::iterator i = find(columns.begin(), columns.end(), column);
    if(i != columns.end()) { values[distance(columns.begin(), i)] = token; }
  }
  out->process_token(token);

  ++column;
}

void calculator::process_line()
{
  if(first_row) {
    for(vector<int>::const_iterator i = columns.begin(); i != columns.end(); ++i)
      if((*i) < 0) throw runtime_error("calculator couldn't find a column");
    out->process_token("ERS_VOLT(1)");
    out->process_token("ERS_VOLT(2)");
    first_row = 0;
  }
  else {
    for(int i = 0; i < 2; ++i) {
      double v1;
      double v2;
      bool blank = 0;
      { istringstream ss(values[i]); ss >> v1; if(!ss || v1 < 0.0) blank = 1; }
      { istringstream ss(values[i + 2]); ss >> v2; if(!ss || v2 < 0.0) blank = 1; }
      if(blank) out->process_token("");
      else { stringstream ss; ss << v1 - v2; out->process_token(ss.str().c_str()); }
    }
  }
  out->process_line();
  column = 0;
}

double filter(double i)
{
  if(!isnan(i) && (i < -500.0 || i > 500.0)) return numeric_limits<double>::quiet_NaN();
  else return i;
}

double sum(double a, double b)
{
  return a + b;
}

double calc(double a, double b)
{
  if(isnan(a) || isnan(b)) return numeric_limits<double>::quiet_NaN();
  else return a * 13.0 + b;
}

////////////////////////////////////////////////////////////////////////////////////////////////
// main
////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char * argv[])
{
  int ret_val = 0;

  try {
    csv_file_writer w("data.csv");

    //ordered_tee t(w, w2);
    //tee t(w, w2);

    //differ d(w, "data C11", "data J71", "delta");
    //differ d2(d, "data C10", "data D10", "delta Trims");

    //unary_col_modifier<double (*)(double)> um(w);
    //um.add("BIN_OM_INIT_GOOD_MBLKS_PLANE0", filter);

    //unary_col_adder<double (*)(double)> ua(w);
    //ua.add("BIN_OM_INIT_GOOD_MBLKS_PLANE0", "\\0_FILTER", filter);

    binary_col_modifier<double (*)(double, double)> bm(w);
    bm.add("NPT\\d+", "MIN_DAC_VOLTAGE", calc);

    //variance_analyzer a(w);
    //a.add_group("^LOT$");
    //a.add_group("^WAFER$");
    //a.add_exception("^ROW$");
    //a.add_exception("^COL$");
    //a.add_exception("^WAFSIZE$");
    //a.add_exception("^Process_id$");
    //a.add_exception("^Fail_bin$");
    //a.add_exception("^Error_bin$");
    //a.add_group("^Group$");
    //a.add_exception("^MAP_REV$");
    //a.add_data(".*");

    //substitutor sb(w);
    //sb.add_exception("^LOT$");
    //sb.add_exception("^WAFER$");
    //sb.add_exception("^ROW$");
    //sb.add_exception("^COL$");
    //sb.add_exception("^WAFSIZE$");
    //sb.add_exception("^Process_id$");
    //sb.add_exception("^Fail_bin$");
    //sb.add_exception("^Error_bin$");
    //sb.add_exception("^Group$");
    //sb.add(".*", "^(\\d)(\\d+)$", "\\1.\\2");

    //col_adder ca(w);
    //ca.add_exception("^LOT$");
    //ca.add_exception("^WAFER$");
    //ca.add_exception("^ROW$");
    //ca.add_exception("^COL$");
    //ca.add_exception("^WAFSIZE$");
    //ca.add_exception("^Process_id$");
    //ca.add_exception("^Fail_bin$");
    //ca.add_exception("^Error_bin$");
    //ca.add_exception("^Group$");
    //ca.add(".*", "\\0_NEW", "^(\\d)(\\d+)$", "\\1.\\2");

    //summarizer su(w);
    //su.add_group("^LOT$");
    //su.add_group("^WAFER$");
    //su.add_exception("^ROW$");
    //su.add_exception("^COL$");
    //su.add_exception("^WAFSIZE$");
    //su.add_exception("^Process_id$");
    //su.add_exception("^Fail_bin$");
    //su.add_exception("^Error_bin$");
    //su.add_group("^Group$");
    //su.add_exception("^MAP_REV$");
    //su.add_group("^keyword$");
    //su.add_data(".*", SUM_AVG);

    //splitter sp(w, SP_REMOVE);
    //sp.add_action(0, "LOT", SP_GROUP);
    //sp.add_action(0, "WAFER", SP_GROUP);
    //sp.add_action(0, "ROW", SP_GROUP);
    //sp.add_action(0, "COL", SP_GROUP);
    //sp.add_action(0, "keyword", SP_GROUP);
    //sp.add_action(0, "Group", SP_SPLIT_BY);
    //sp.add_action(0, "data", SP_SPLIT);
    //sp.add_action(0, "Fail_bin", SP_SPLIT);
    //sp.add_action(0, "Error_bin", SP_SPLIT);

    //stacker st(w, ST_STACK);
    //st.add_action(0, "LOT", ST_LEAVE);
    //st.add_action(0, "WAFER", ST_LEAVE);
    //st.add_action(0, "ROW", ST_LEAVE);
    //st.add_action(0, "COL", ST_LEAVE);
    //st.add_action(0, "WAFSIZE", ST_REMOVE);
    //st.add_action(0, "Process_id", ST_LEAVE);
    //st.add_action(0, "Fail_bin", ST_LEAVE);
    //st.add_action(0, "Error_bin", ST_LEAVE);
    //st.add_action(0, "Group", ST_LEAVE);
    //st.add_action(0, "MAP_REV", ST_LEAVE);

    //calculator ca(st);

    //combiner c(w);
    //c.add_pair("^BIN_O_FTR_(.*BG)$", "BIN_OO_\\1");

    //subset_tee s(w);
    //s.add_data(1, ".*");
    //s.add_exception(1, "WLSV_OFST_VPGM_WLGRP1.*");
    //s.set_dest(w2);
    //s.add_data(1, "WLSV_OFST_VPGM_WLGRP1.*");
    //s.set_dest(w3);
    //s.add_data(1, "PGM_OTP.*");
    //s.add_exception(0, "PGM_OTP_MAIN_TRIM0(43)");

    threader th(bm);

    base_converter bc(th, "NPT\\d+", 16, 10);

    //col_pruner cp(w);

    read_csv("raw.csv", bc);

    //row_joiner rj(w);

    //read_csv("test1.csv", rj);
    //read_csv("test2.csv", rj);
    //rj.process_lines();
    //read_csv("test3.csv", rj);
    //read_csv("test4.csv", rj);
    //rj.process();
  }
  catch(exception& e) { cerr << "Exception: " << e.what() << endl; }
  catch(...) { cerr << "Unknown Exception" << endl; }

  return ret_val;
}

