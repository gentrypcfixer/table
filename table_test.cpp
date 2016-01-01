#include <windows.h>
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

void copy(const char* from, const char* to)
{
  const size_t read_size = 256 * 1024;
  char buf[read_size];
  int ffd = open(from, O_RDONLY | O_BINARY); if(ffd < 0) throw runtime_error("can't open input file");
  int tfd = open(to, O_WRONLY | O_TRUNC | O_CREAT | O_BINARY, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH); if(tfd < 0) throw runtime_error("can't open output file");
  size_t num_read;
  do {
    num_read = read(ffd, buf, read_size);
    const char* begin = buf;
    ssize_t rem = num_read;
    while(rem) {
      ssize_t num_written = write(tfd, begin, rem);
      begin += num_written;
      rem -= num_written;
    }
  } while(num_read > 0);

  close(ffd);
  close(tfd);
}

void copy2(const char* from, const char* to)
{
  const DWORD read_size = 1024 * 1024;
  char buf[read_size];
  HANDLE ffd = CreateFile(from, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL); if(ffd == INVALID_HANDLE_VALUE) throw runtime_error("can't open input file");
  HANDLE tfd = CreateFile(to, GENERIC_WRITE, FILE_SHARE_READ, NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL); if(tfd == INVALID_HANDLE_VALUE) throw runtime_error("can't open output file");
  DWORD num_read;
  while(ReadFile(ffd, buf, read_size, &num_read, NULL) && num_read > 0)
  {
    cout << "reading\n";
    const char* begin = buf;
    DWORD rem = num_read;
    while(rem) {
      DWORD num_written; WriteFile(tfd, begin, rem, &num_written, NULL);
      begin += num_written;
      rem -= num_written;
    }
  }

  CloseHandle(ffd);
  CloseHandle(tfd);
}

int main(int argc, char * argv[])
{
  int ret_val = 0;

  try {
    //const char* default_format = "-format=log,tstvr,rev,dsid,pt,uin,upass,e_start,e_stop,tstr,card";
    //for(arg_fetcher af(default_format, always_split_arg); af.type(); af.get_next()) { process_arg(af); }

    //csv_file_reader<stacker<csv_file_writer> > r; r.open("data.csv");
    //stacker<csv_file_writer>& s = r.get_out(); s.set_default_action(ST_LEAVE); s.add_action(1, "NPT\\d+", ST_STACK);
    //csv_file_writer& w = s.get_out(); w.open("out.csv");
    //r.run();

    copy2("data.csv", "out.csv");

    //csv_file_reader<base_converter<binary_col_adder<csv_file_writer> > > r; r.open("raw.csv");
    //base_converter<binary_col_adder<csv_file_writer> >& bc = r.get_out(); bc.add_conv("NPT\\d+", 16, 10);
    //binary_col_adder<csv_file_writer>& ba = bc.get_out(); ba.add("NPT\\d+", "MIN_DAC_VOLTAGE", "\\0", calc, 1);
    //csv_file_writer& w = ba.get_out(); w.open("data.csv");
    //csv_file_reader<csv_file_writer> r; r.open("data.csv");
    //csv_file_writer& w = r.get_out(); w.open("out.csv");
    //r.run();
  }
  catch(exception& e) { cerr << "Exception: " << e.what() << endl; }
  catch(...) { cerr << "Unknown Exception" << endl; }

  return ret_val;
}

