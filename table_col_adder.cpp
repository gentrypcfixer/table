#include <iostream>
#include <stdexcept>
#include "table.h"

using namespace std;
using namespace table;

void print_help()
{
  cout << "table_col_adder col_regex new_col_name from to\n";
  cout << "    col_regex      search expression used to select input column\n";
  cout << "    new_col_name   name of the column to add, can use \\d notition to base it on input column name\n";
  cout << "    from           regular expression used to capture input data\n";
  cout << "    to             output data\n";
}

int main(int argc, char* argv[])
{
  int ret_val = 0;

  try {
    const char* col_regex = 0;
    const char* new_col_name = 0;
    const char* from = 0;
    const char* to = 0;
    int mode = 0;
    for(int arg = 1; arg < argc; ++arg) {
      if('-' == argv[arg][0]) {
        if('h' == argv[arg][1]) { print_help(); exit(0); }
        else { throw runtime_error("invalid argument"); }
      }
      else {
        if(mode == 0) { col_regex = argv[arg]; ++mode; }
        else if(mode == 1) { new_col_name = argv[arg]; ++mode; }
        else if(mode == 2) { from = argv[arg]; ++mode; }
        else if(mode == 3) { to = argv[arg]; ++mode; }
        else { throw runtime_error("invalid argument"); }
      }
    }

    csv_writer w(cout.rdbuf());
    substituter sub(from, to);
    basic_unary_col_adder<c_str_and_len_t, c_str_and_len_t, substituter> suba(w);
    suba.add(col_regex, new_col_name, sub);
    read_csv(cin.rdbuf(), suba);
  }
  catch(exception& e) { cerr << "Exception: " << e.what() << endl; }
  catch(...) { cerr << "Unknown Exception" << endl; }

  return ret_val;
}

