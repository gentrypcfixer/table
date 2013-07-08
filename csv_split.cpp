#include <string>
#include <vector>
#include <set>
#include <iostream>
#include <sstream>
#include "csv_row.hpp"

using namespace std;

enum field_action_e
{
  SPLIT_BY,
  SPLIT,
  GROUP,
  THROW
};


struct split_info_t
{
  string name;
  string temp_file_name;
};

int main(const unsigned int argc, char const * const argv[])
{
  int ret_val = 0;
  string split_by_field_name = "Group";
  set<string> field_names_to_split;
  set<string> field_names_to_group;
  set<string> field_names_to_throw;

  //first pass
  //  seperate data into files per group
  //  remove WAFSIZE and Process_id
  //  put Label right after COL, so all group by fields are together
  //Second pass
  //  while data left
  //    read a line of each temp file
  //    insert data to map
  //    if map full for that key, print
  //  print remaining data
  //
  CSV_Row_t in_field_names;
  cin >> in_field_names;

  vector<field_action_e> field_action;
  for(CSV_Row_t::iterator i = in_field_names.begin(); in_field_names.end() != i; ++i) {
    if(split_by_field_name == (*i)) { field_action.push_back(SPLIT_BY); split_by_field = i-in_field_names.begin(); }
    else if(field_names_to_split.end() != field_names_to_split.find(*i)) field_action.push_back(SPLIT);
    else if(field_names_to_group.end() != field_names_to_group.find(*i)) field_action.push_back(GROUP);
    else if(field_names_to_throw.end() != field_names_to_throw.find(*i)) field_action.push_back(THROW);
    else if(0 == (*i).compare("Data")) field_action.push_back(SPLIT);
    else if(0 == (*i).compare("LOT") ||
         0 == (*i).compare("WAFER") ||
         0 == (*i).compare("ROW") ||
         0 == (*i).compare("COL")  )
      field_action.push_back(GROUP);
  }

  {
    string line;
    getline(cin, line);

    size_t cur_pos = 0;
    for(int i = 0; i < 9; ++i)
      cur_pos = line.find_first_of(',', cur_pos) + 1;
    cout << line.substr(0, cur_pos) << "Label,Data\n";

    while(1) {
      size_t next_delim = line.find_first_of(',', cur_pos);
      if(string::npos == next_delim) {
        labels.push_back(line.substr(cur_pos));
        break;
      }
      else {
        labels.push_back(line.substr(cur_pos, next_delim - cur_pos));
        cur_pos = next_delim + 1;
      }
    }
  }

  do {
    string line;
    getline(cin, line);
    if(cin.fail())
      break;

    size_t cur_pos = 0;
    for(int i = 0; i < 9; ++i)
      cur_pos = line.find_first_of(',', cur_pos) + 1;
    string prefix = line.substr(0, cur_pos);
    for(size_t field = 0; 1; ++field) {
      size_t next_delim = line.find_first_of(',', cur_pos);
      if(string::npos == next_delim) {
        cout << prefix << labels[field] << ',' << line.substr(cur_pos) << "\n";
        break;
      }
      else {
        cout << prefix << labels[field] << ',' << line.substr(cur_pos, next_delim - cur_pos) << "\n";
        cur_pos = next_delim + 1;
      }
    }
  } while(cin.good());

  cout << flush;

  return ret_val;
}

