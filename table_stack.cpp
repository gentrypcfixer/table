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
    csv_reader<stacker<csv_writer> > r; r.set_fd(STDIN_FILENO);
    stacker<csv_writer>& s = r.get_out(); s.set_default_action(ST_LEAVE);
    csv_writer& w = s.get_out(); w.set_fd(STDOUT_FILENO);

    stack_action_e mode = ST_STACK;
    bool regex = 0;
    for(arg_fetcher af(argc - 1, argv + 1, always_split_arg); af.type(); af.get_next()) {
      if(af.type() & st_ddash) {
        if(!af.key()) { throw runtime_error("invalid double dash argument"); }
        else { throw runtime_error("invalid double dash argument: " + string(af.key(), af.key_len())); }
      }
      else if(af.type() & st_plus) {
        if(!af.key()) { throw runtime_error("invalid plus argument"); }
        else { throw runtime_error("invalid plus argument: " + string(af.key(), af.key_len())); }
      }
      else if(af.type() & st_dash) {
        if(!af.key()) { throw runtime_error("invalid dash argument"); }
        else if('h' == af.key()[0]) { print_help(); exit(0); }
        else if('s' == af.key()[0]) { mode = ST_STACK; regex = 0; }
        else if('S' == af.key()[0]) { mode = ST_STACK; regex = 1; }
        else if('r' == af.key()[0]) { mode = ST_REMOVE; regex = 0; }
        else if('R' == af.key()[0]) { mode = ST_REMOVE; regex = 1; }
        else if('l' == af.key()[0]) { mode = ST_LEAVE; regex = 0; }
        else if('L' == af.key()[0]) { mode = ST_LEAVE; regex = 1; }
        else { throw runtime_error("invalid dash argument: " + string(af.key(), af.key_len())); }
      }
      else s.add_action(regex, af.val(), mode);
    }

    r.run();
  }
  catch(exception& e) { cerr << "Exception: " << e.what() << endl; }
  catch(...) { cerr << "Unknown Exception" << endl; }

  return ret_val;
}

