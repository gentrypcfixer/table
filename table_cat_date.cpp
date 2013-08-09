#include <dirent.h>
#include <errno.h>
#include <string>
#include <map>
#include <iostream>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <pcrecpp.h>

using namespace std;
using namespace pcrecpp;

int main(int argc, char * argv[])
{
  int ret_val = 0;
  char const* out_dir = ".";

  for(int arg = 1; arg < argc; ++arg) {
    if('-' == argv[arg][0]) {
      if('o' == argv[arg][1]) {
        out_dir = argv[++arg];
      }
    }
  }

  map<string,ofstream*> out_files;
  try {
    DIR* dir = opendir("."); if(!dir) throw runtime_error("process_directory: Can't opendir");
    RE file_regex( "(\\d+_\\d+_\\d+)_(.*)\\.csv" );
    while(1) {
      errno = 0; dirent* dp = readdir(dir);
      if(!dp) break;

      string date;
      string file;
      if(file_regex.FullMatch(dp->d_name, &date, &file)) {
        ifstream in;
        in.exceptions(ifstream::badbit);
        in.open(dp->d_name);

        map<string,ofstream*>::iterator i = out_files.find(file);
        if(out_files.end() == i) {
          pair<map<string,ofstream*>::iterator,bool> ret = out_files.insert(pair<string,ofstream*>(file, new ofstream));
          if(!ret.second) throw runtime_error("Can't add to map");
          i = ret.first;

          stringstream out_path;
          out_path << out_dir << "/" << file << ".csv";
          (*i).second->exceptions(ifstream::badbit);
          (*i).second->open(out_path.str().c_str(), ofstream::out|ofstream::trunc);

          string line;
          getline(in, line);
          (*(*i).second) << "DATE," << line << endl;
        }
        else {
          string line;
          getline(in, line);
        }

        ofstream& out = *(*i).second;
        do {
          string line;
          getline(in, line);
          if(!in.fail())
            out << date << "," << line << "\n";
        } while(in.good());

        in.close();
      }
    }

    if(0 != errno) throw runtime_error("Can't readdir");

    closedir(dir);
  }
  catch(exception& e) {
    cerr << "Exception: " << e.what() << endl;
    ret_val = 1;
  }

  for(map<string,ofstream*>::iterator i = out_files.begin(); i != out_files.end(); ++i)
    delete (*i).second;

  return ret_val;
}

