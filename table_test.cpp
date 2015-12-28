#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
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

double calc(double a, double b)
{
  if(isnan(a) || isnan(b)) return numeric_limits<double>::quiet_NaN();
  else return a * 13.0 + b;
}

////////////////////////////////////////////////////////////////////////////////////////////////
// main
////////////////////////////////////////////////////////////////////////////////////////////////
void process_arg(arg_fetcher& af)
{
  while(af.type()) {
    cout << string(af.key(), af.key_len()) << ' ' << string(af.val(), af.val_len()) << endl;

    if(af.type() & st_more_split_vals) af.get_next();
    else break;
  }
}

int main(int argc, char * argv[])
{
  int ret_val = 0;

  try {
    //const char* default_format = "-format=log,tstvr,rev,dsid,pt,uin,upass,e_start,e_stop,tstr,card";
    //for(arg_fetcher af(default_format, always_split_arg); af.type(); af.get_next()) { process_arg(af); }

    //csv_file_reader<base_converter<binary_col_adder<csv_file_writer> > > r; r.open("raw.csv");
    //base_converter<binary_col_adder<csv_file_writer> >& bc = r.get_out(); bc.add_conv("NPT\\d+", 16, 10);
    //binary_col_adder<csv_file_writer>& ba = bc.get_out(); ba.add("NPT\\d+", "MIN_DAC_VOLTAGE", "\\0", calc, 1);
    //csv_file_writer& w = ba.get_out(); w.open("data.csv");
    csv_file_reader<csv_writer> r; r.open("data.csv");
    csv_writer& w = r.get_out(); w.set_fd(STDOUT_FILENO);
    r.run();
  }
  catch(exception& e) { cerr << "Exception: " << e.what() << endl; }
  catch(...) { cerr << "Unknown Exception" << endl; }

  return ret_val;
}

