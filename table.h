#define use_unordered

#include <string>
#ifdef use_unordered
# include <tr1/unordered_map>
#endif
#include <map>
#include <set>
#include <list>
#include <vector>
#include <iostream>
#include <sstream>
#include <pcrecpp.h>


#ifdef use_unordered
namespace std { namespace tr1 {
template<> struct hash<vector<string> >
{
  hash<string> h;
  size_t operator()(const vector<string>& arg) const {
    stringstream ss;
    for(vector<string>::const_iterator i = arg.begin(); i != arg.end(); ++i)
      ss << *i;
    return h(ss.str());
  }
};
}}


namespace table {

////////////////////////////////////////////////////////////////////////////////////////////////
// cstring_queue
////////////////////////////////////////////////////////////////////////////////////////////////

class cstring_queue
{
  struct block_t {
    struct block_data_t {
      size_t rc;
      size_t cap;
      char* buf;
      char* last_token;
      char* next_write;
    } *data;

    block_t() : data(0) {}
    block_t(size_t cap) : data(0) { init(cap); }
    block_t(const block_t& other) { data = other.data; if(data) ++data->rc; }
    ~block_t() { if(data && !(--data->rc)) { delete[] data->buf; delete data; } }

    block_t& operator=(const block_t& other) {
      if(data && !(--data->rc)) { delete[] data->buf; delete data; }
      data = other.data;
      if(data) ++data->rc;
      return *this;
    }

    void init(size_t cap) {
      if(data && !(--data->rc)) { delete[] data->buf; delete data; }
      data = new block_data_t;
      data->rc = 1;
      data->cap = cap;
      data->buf = new char[cap];
    }
  };

  static std::vector<block_t> free_blocks;
  struct cstring_queue_data_t {
    size_t rc;
    std::vector<block_t> blocks;
    char* next_read;
  }* data;
  size_t default_block_cap;
  bool cow;

  std::vector<block_t>::iterator add_block(size_t len);
  void copy();

public:
  class const_iterator
  {
    friend class cstring_queue;
    std::vector<block_t>::const_iterator bi;
    std::vector<block_t>::const_iterator bie;
    char* d;

    const_iterator(std::vector<block_t>::const_iterator bi, std::vector<block_t>::const_iterator bie) : bi(bi), bie(bie), d(bi != bie ? (*bi).data->buf : 0) {} //begin
    const_iterator() : d(0) {} //end

  public:
    const char* operator*() { return d; }
    bool operator==(const const_iterator& other) { return d == other.d; }
    bool operator!=(const const_iterator& other) { return d != other.d; };

    const_iterator& operator++() {
      if(!d) return *this;

      if(d == (*bi).data->last_token) {
        if(++bi == bie) d = 0;
        else d = (*bi).data->buf;
      }
      else {
        while(*d != '\0') ++d;
        ++d;
      }
      return *this;
    }
  };

  cstring_queue() { data = new cstring_queue_data_t; data->rc = 1; data->next_read = 0; default_block_cap = 8 * 1024; cow = 0; }
  cstring_queue(const cstring_queue& other) { data = other.data; ++data->rc; default_block_cap = other.default_block_cap; cow = 1; }
  cstring_queue& operator=(const cstring_queue& other) { if(data && !(--data->rc)) { clear(); delete data; } data = other.data; ++data->rc; default_block_cap = other.default_block_cap; cow = 1; return *this; }
  ~cstring_queue() { if(data && !(--data->rc)) { clear(); delete data; } }

  const_iterator begin() const { return data ? const_iterator(data->blocks.begin(), data->blocks.end()) : const_iterator(); }
  const_iterator end() const { return const_iterator(); };
  bool empty() const { return !data->next_read; }
  const char* front() const { return data->next_read; }
  bool operator<(const cstring_queue& other) const {
    if(data == other.data) return false;
    std::vector<block_t>::iterator i = data->blocks.begin(), j = other.data->blocks.begin();
    for(; i != data->blocks.end() && j != other.data->blocks.end(); ++i, ++j) {
      const size_t len = (*i).data->next_write - (*i).data->buf;
      const size_t olen = (*j).data->next_write - (*j).data->buf;
      const size_t clen = len < olen ? len : olen;
      int ret = memcmp((*i).data->buf, (*j).data->buf, clen);
      if(ret < 0) return true;
      else if(ret > 0) return false;
      else {
        if(len < olen) return true;
        else if(len > olen) return false;
      }
    }
    if(j != other.data->blocks.end()) return true;
    return false;
  }
  bool operator==(const cstring_queue& other) const {
    if(data == other.data) return true;
    std::vector<block_t>::iterator i = data->blocks.begin(), j = other.data->blocks.begin();
    for(; i != data->blocks.end() && j != other.data->blocks.end(); ++i, ++j) {
      size_t len = (*i).data->next_write - (*i).data->buf;
      size_t olen = (*j).data->next_write - (*j).data->buf;
      if(len != olen || memcmp((*i).data->buf, (*j).data->buf, len)) return false;
    }
    return true;
  }
  size_t hash() const {
    size_t r = 0;
    for(std::vector<block_t>::const_iterator i = data->blocks.begin(); i != data->blocks.end(); ++i)
      for(char* d = (*i).data->buf; d < (*i).data->next_write; ++d)
        r += *d;
    return r;
  }

  void set_default_cap(size_t cap) { default_block_cap = cap; }
  void clear();
  void push(const char* c) {
    if(cow) copy();
    const size_t len = strlen(c);
    std::vector<block_t>::iterator i = data->blocks.begin();
    if(i == data->blocks.end()) i = add_block(len);
    else {
      std::advance(i, data->blocks.size() - 1);
      const size_t rem = (*i).data->cap - ((*i).data->next_write - (*i).data->buf);
      if(rem <= len) i = add_block(len);
    }
    (*i).data->last_token = (*i).data->next_write;
    memcpy((*i).data->next_write, c, len + 1);
    (*i).data->next_write += len + 1;
  }
  void pop() {
    if(cow) copy();
    std::vector<block_t>::iterator i = data->blocks.begin();
    if(i == data->blocks.end()) return;
    if(data->next_read != (*i).data->last_token) {
      while(*data->next_read != '\0') ++data->next_read;
      ++data->next_read;
    }
    else {
      free_blocks.insert(free_blocks.end(), *i);
      i = data->blocks.erase(i);
      if(i != data->blocks.end()) data->next_read = (*i).data->buf;
      else { data->next_read = 0; }
    }
  }
};



////////////////////////////////////////////////////////////////////////////////////////////////
// pass
////////////////////////////////////////////////////////////////////////////////////////////////

class pass {
public:
  virtual ~pass();

  virtual void process_token(const char* token) = 0;
  virtual void process_line() = 0;
  virtual void process_stream() = 0;
};


////////////////////////////////////////////////////////////////////////////////////////////////
// subset_tee
////////////////////////////////////////////////////////////////////////////////////////////////

struct subset_tee_dest_data_t
{
  std::set<std::string> key;
  std::set<std::string> key_except;
  std::vector<pcrecpp::RE*> regex;
  std::vector<pcrecpp::RE*> regex_except;
  bool has_data;

  subset_tee_dest_data_t() : has_data(0) {}
  ~subset_tee_dest_data_t();
};

class subset_tee : public pass {
  std::map<pass*, subset_tee_dest_data_t> dest_data;
  std::map<pass*, subset_tee_dest_data_t>::iterator ddi;
  bool first_row;
  int column;
  std::vector<std::pair<int, pass*> > dest;
  std::vector<std::pair<int, pass*> >::iterator di;

public:
  subset_tee();
  subset_tee(pass& dest);
  void init(pass& dest);
  subset_tee& set_dest(pass& dest);
  subset_tee& add_data(bool regex, const char* key);
  subset_tee& add_exception(bool regex, const char* key);

  void process_token(const char* token);
  void process_line();
  void process_stream();
};


////////////////////////////////////////////////////////////////////////////////////////////////
// ordered_tee
////////////////////////////////////////////////////////////////////////////////////////////////

class ordered_tee : public pass {
  std::vector<pass*> out;
  bool first_row;
  int num_columns;
  cstring_queue data;

public:
  ordered_tee();
  ordered_tee(pass& out1, pass& out1);
  ~ordered_tee();
  void init(pass& out1, pass& out2);
  void add_out(pass& out);

  void process_token(const char* token);
  void process_line();
  void process_stream();
};


////////////////////////////////////////////////////////////////////////////////////////////////
// summarizer
////////////////////////////////////////////////////////////////////////////////////////////////

enum summarizer_flags {
  SUM_MISSING =  0x0002,
  SUM_COUNT =    0x0004,
  SUM_SUM =      0x0008,
  SUM_MIN =      0x0010,
  SUM_MAX =      0x0020,
  SUM_AVG =      0x0040,
  SUM_VARIANCE = 0x0080,
  SUM_STD_DEV =  0x0100
};

struct summarizer_data_t
{
  int missing;
  int count;
  double sum;
  double sum_of_squares;
  double min;
  double max;

  summarizer_data_t();
};

class summarizer : public pass {
  pass* out;
  std::vector<pcrecpp::RE*> group_regexes;
  std::vector<std::pair<pcrecpp::RE*, uint32_t> > data_regexes;

  bool first_line;
  std::vector<uint32_t> column_flags;
  int num_data_columns;

  std::vector<uint32_t>::const_iterator cfi;
  cstring_queue group_tokens;
  double* values;
  double* vi;
  std::map<cstring_queue, summarizer_data_t*> data;

public:
  summarizer();
  summarizer(pass& out);
  summarizer& init(pass& out);
  ~summarizer();

  summarizer& add_group(const char* regex);
  summarizer& add_data(const char* regex, uint32_t flags);

  void process_token(const char* token);
  void process_line();
  void process_stream();
};


////////////////////////////////////////////////////////////////////////////////////////////////
// stacker
////////////////////////////////////////////////////////////////////////////////////////////////

enum stack_action_e
{
  ST_STACK,
  ST_LEAVE,
  ST_REMOVE
};

struct regex_stack_action_t
{
  pcrecpp::RE* regex;
  stack_action_e action;

  regex_stack_action_t() : regex(0) {}
  ~regex_stack_action_t() { delete regex; }
};

class stacker : public pass {
  pass* out;
  stack_action_e default_action;
  std::map<std::string, stack_action_e> keyword_actions;
  std::vector<regex_stack_action_t> regex_actions;

  //header information
  bool first_line;
  std::vector<std::string> stack_keys;
  std::vector<stack_action_e> actions;
  size_t last_leave;

  //current line information
  size_t column;
  size_t stack_column;
  cstring_queue leave_tokens;
  cstring_queue stack_tokens;

public:
  stacker();
  stacker(pass& out, stack_action_e default_action);
  ~stacker();

  void re_init();
  stacker& init(pass& out, stack_action_e default_action);
  stacker& add_action(bool regex, const char* key, stack_action_e action);

  void process_token(const char* token);
  void process_line();
  void process_stream();
};


////////////////////////////////////////////////////////////////////////////////////////////////
// splitter
////////////////////////////////////////////////////////////////////////////////////////////////

enum split_action_e
{
  SP_GROUP,
  SP_SPLIT_BY,
  SP_SPLIT,
  SP_REMOVE
};

struct regex_split_action_t
{
  pcrecpp::RE* regex;
  split_action_e action;

  regex_split_action_t() : regex(0) {}
  ~regex_split_action_t() { delete regex; }
};

class splitter : public pass {
  pass* out;
  split_action_e default_action;
  std::map<std::string, split_action_e> keyword_actions;
  std::vector<regex_split_action_t> regex_actions;

  bool first_line;
  std::vector<split_action_e> actions;
  std::vector<std::string> group_keys;
  std::vector<std::string> split_keys;

  size_t column;
  std::vector<std::string> group_tokens;
  std::vector<std::string> split_by_tokens;
  std::vector<std::string> split_tokens;

  std::map<std::string, size_t> out_split_keys;
#ifdef use_unordered
  std::tr1::unordered_map<std::vector<std::string>, std::vector<std::string> > data;
#else
  std::map<std::vector<std::string>, std::vector<std::string> > data;
#endif

public:
  splitter();
  splitter(pass& out, split_action_e default_action);
  ~splitter();

  void re_init();
  splitter& init(pass& out, split_action_e default_action);
  splitter& add_action(bool regex, const char* key, split_action_e action);

  void process_token(const char* token);
  void process_line();
  void process_stream();
};


////////////////////////////////////////////////////////////////////////////////////////////////
// row_joiner
////////////////////////////////////////////////////////////////////////////////////////////////

class row_joiner : public pass {
  pass* out;
  std::vector<std::string> table_name;
  size_t table;

  bool first_line;
  bool more_lines;
  size_t column;
  std::vector<size_t> num_columns;
  std::vector<std::string> keys;

  std::vector<cstring_queue> data;

public:
  row_joiner();
  row_joiner(pass& out, const char* table_name = 0);
  row_joiner& init(pass& out, const char* table_name = 0);
  row_joiner& add_table_name(const char* name = 0);

  void process_token(const char* token);
  void process_line();
  void process_stream();
  void process_lines();
  void process();
};


////////////////////////////////////////////////////////////////////////////////////////////////
// col_match_joiner
////////////////////////////////////////////////////////////////////////////////////////////////

#if 0
struct col_match_joiner_group_storage_t {
  size_t cap;
  char* buf;
  char* next_write;
};

struct col_match_joiner_data_storage_t {
  uint32_t start_index;
  size_t cap;
  char* buf;
};

struct column_info_t {
  char type; // 0 = remove/ignore, 1 = group, 2 = data
  int index;
};

class col_match_joiner : public pass {
  //setup info
  pass* out;
  std::map<std::string, uint32_t> group_col;
  int end_stream;

  //incoming table info
  int cur_stream;
  int cur_stream_column;
  std::vector<column_info_t> column_info;
  const char* cur_group;
  const char* cur_group_cap;

  //outgoing table info
  std::map<std::string> keys;
  std::vector<col_match_joiner_group_storage_t> group_storage;
  size_t next_group_row;
  std::tr1::unordered_map<const char*, size_t> group_row;
  std::vector<std::vector<col_match_joiner_data_storage_t> > data_storage; //column then block of data

public:
  col_match_joiner();
  col_match_joiner(pass& out, split_action_e default_action);
  ~col_match_joiner();

  void re_init();
  col_match_joiner& init(pass& out, split_action_e default_action);
  col_match_joiner& add_action(bool regex, const char* key, split_action_e action);

  void process_token(const char* token);
  void process_line();
  void process_stream();
};
#endif


////////////////////////////////////////////////////////////////////////////////////////////////
// filter
////////////////////////////////////////////////////////////////////////////////////////////////

class filter : public pass {
  struct limits_t
  {
    double low_limit;
    double high_limit;
  };

  struct regex_limits_t
  {
    pcrecpp::RE* regex;
    limits_t limits;

    regex_limits_t() : regex(0) {}
    ~regex_limits_t() { delete regex; }
  };

  pass* out;
  std::map<std::string, limits_t> keyword_limits;
  std::vector<regex_limits_t> regex_limits;
  bool first_row;
  int column;
  std::vector<std::pair<int, limits_t> > column_limits;
  std::vector<std::pair<int, limits_t> >::const_iterator cli;

public:
  filter();
  filter(pass& out);
  void init(pass& out);
  void add(bool regex, const char* keyword, double low_limit, double high_limit);

  void process_token(const char* token);
  void process_line();
  void process_stream();
};


////////////////////////////////////////////////////////////////////////////////////////////////
// col_pruner
////////////////////////////////////////////////////////////////////////////////////////////////

class col_pruner : public pass {
  pass* out;
  bool first_row;
  bool passthrough;
  size_t column;
  size_t num_columns;
  size_t columns_with_data;
  uint32_t* has_data;
  cstring_queue data;

public:
  col_pruner();
  col_pruner(pass& out);
  void init(pass& out);

  void process_token(const char* token);
  void process_line();
  void process_stream();
};


////////////////////////////////////////////////////////////////////////////////////////////////
// combiner
////////////////////////////////////////////////////////////////////////////////////////////////

class combiner : public pass {
  pass* out;
  std::vector<std::pair<std::string, std::string> > pairs;
  bool first_row;
  int column;
  std::vector<int> remap_indexes;
  std::vector<string> tokens;

public:
  combiner();
  combiner(pass& out);
  void init(pass& out);
  void add_pair(const char* from, const char* to);

  void process_token(const char* token);
  void process_line();
  void process_stream();
};


////////////////////////////////////////////////////////////////////////////////////////////////
// differ
////////////////////////////////////////////////////////////////////////////////////////////////

class differ : public pass {
  pass* out;
  std::string base_key;
  std::string comp_key;
  std::string keyword;
  bool first_row;
  int base_column;
  int comp_column;

  int column;
  bool blank;
  double base_value;
  double comp_value;

public:
  differ();
  differ(pass& out, const char* base_key, const char* comp_key, const char* keyword);
  ~differ();
  void init(pass& out, const char* base_key, const char* comp_key, const char* keyword);

  void process_token(const char* token);
  void process_line();
  void process_stream();
};


////////////////////////////////////////////////////////////////////////////////////////////////
// base_converter
////////////////////////////////////////////////////////////////////////////////////////////////

class base_converter : public pass {
  struct regex_base_conv_t
  {
    pcrecpp::RE* regex;
    int from;
    int to;

    regex_base_conv_t() : regex(0) {}
    ~regex_base_conv_t() { delete regex; }
  };

  pass* out;
  std::vector<regex_base_conv_t> regex_base_conv;
  bool first_row;
  int column;
  std::vector<int> from_base;
  std::vector<int> to_base;

public:
  base_converter();
  base_converter(pass& out, const char* regex, int from, int to);
  ~base_converter();
  void init(pass& out, const char* regex, int from, int to);
  base_converter& add_conv(const char* regex, int from, int to);

  void process_token(const char* token);
  void process_line();
  void process_stream();
};


////////////////////////////////////////////////////////////////////////////////////////////////
// writer
////////////////////////////////////////////////////////////////////////////////////////////////

class writer_base : public pass {
protected:
  bool first_column;
  std::streambuf* out;

  writer_base() : out(0) {}

public:
  void process_token(const char* token);
  void process_line();
};

class writer : public writer_base {
public:
  writer();
  writer(std::streambuf* out);
  void init(std::streambuf* out);

  void process_stream();
};

class file_writer : public writer_base {
public:
  file_writer();
  file_writer(const char* filename);
  void init(const char* filename);
  ~file_writer();

  void process_stream();
};


////////////////////////////////////////////////////////////////////////////////////////////////
// driver
////////////////////////////////////////////////////////////////////////////////////////////////

void read_csv(std::streambuf* in, pass& out);
void read_csv(const char* filename, pass& out);

}

#ifdef use_unordered
namespace std { namespace tr1 {
template<> struct hash<table::cstring_queue>
{
  size_t operator()(const table::cstring_queue& arg) const { return arg.hash(); }
};
}}
#endif


#endif

