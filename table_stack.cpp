#include <iostream>
#include <stdexcept>
#include "table.h"

using namespace std;
using namespace table;

void print_help()
{
  cout << "table_stack [options]\n";
  cout << "    -h       display this help\n";
  cout << "    -s       columns matching following arguments should be stacked\n";
  cout << "    -S       columns mathcing following arguments (regexes) should be stacked\n";
  cout << "    -r       columns matching following arguments should be removed\n";
  cout << "    -R       columns mathcing following arguments (regexes) should be removed\n";
  cout << "    -l       columns matching following arguments should be left in the output\n";
  cout << "    -L       columns mathcing following arguments (regexes) should be left in the output\n";
  cout << '\n';
  cout << "    columns not mentioned in the arguments will be left in the output\n";
}


int main(int argc, char* argv[])
{
  int ret_val = 0;

  try {
    csv_writer w(cout.rdbuf());
    stacker s(w, ST_LEAVE);

    stack_action_e mode = ST_STACK;
    bool regex = 0;
    for(int arg = 1; arg < argc; ++arg) {
      if('-' == argv[arg][0]) {
        if('h' == argv[arg][1]) { print_help(); exit(0); }
        if('s' == argv[arg][1]) { mode = ST_STACK; regex = 0; }
        else if('S' == argv[arg][1]) { mode = ST_STACK; regex = 1; }
        else if('r' == argv[arg][1]) { mode = ST_REMOVE; regex = 0; }
        else if('R' == argv[arg][1]) { mode = ST_REMOVE; regex = 1; }
        else if('l' == argv[arg][1]) { mode = ST_LEAVE; regex = 0; }
        else if('L' == argv[arg][1]) { mode = ST_LEAVE; regex = 1; }
        else { throw runtime_error("invalid argument"); }
      }
      else s.add_action(regex, argv[arg], mode);
    }

    read_csv(cin.rdbuf(), s);
  }
  catch(exception& e) { cerr << "Exception: " << e.what() << endl; }
  catch(...) { cerr << "Unknown Exception" << endl; }

  return ret_val;
}

