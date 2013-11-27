#ifndef table_h_
#define table_h_

#include <string>
#include <tr1/unordered_map>
#include <map>
#include <set>
#include <vector>
#include <pcre.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>


#if(PCRE_MAJOR < 8 || (PCRE_MAJOR == 8 && PCRE_MINOR < 20))
#define PCRE_STUDY_JIT_COMPILE 0
inline void pcre_free_study(void *p) { free(p); }
#endif


namespace table {

void resize_buffer(char*& buf, char*& next, char*& end, char** resize_end = 0);
void generate_substitution(const char* token, const char* replace_with, const int* ovector, int num_captured, char*& buf, char*& next, char*& end);

struct cstr_less {
  bool operator()(char* const& lhs, char* const& rhs) const { return 0 > strcmp(lhs, rhs); }
};

struct multi_cstr_less { //for multiple null terminated strings terminated by a END OF TEXT (number 3)
  bool operator()(char* const& lhs, char* const& rhs) const {
    const char* l = lhs;
    const char* r = rhs;

    while(*l == *r && *l != '\x03') { ++l; ++r; }

    if(*l < *r) return 1;
    else return 0;
  }
};

struct multi_cstr_hash { //for multiple null terminated strings terminated by a END OF TEXT (number 3)
  size_t operator()(char* const& arg) const {
    size_t ret_val = static_cast<size_t>(2166136261UL);

    size_t temp = 0;
    for(const char* p = arg; *p != '\x03'; ++p) {
      temp = (temp << 8) | *p;
      ret_val = (ret_val ^ temp) * 16777619UL;
    }

    return ret_val;
  }
};

struct multi_cstr_equal_to { //for multiple null terminated strings terminated by a END OF TEXT (number 3)
  bool operator()(char* const& lhs, char* const& rhs) const {
    const char* l = lhs;
    const char* r = rhs;

    while(*l == *r && *l != '\x03') { ++l; ++r; }

    if(*l == '\x03' && *r == '\x03') return 1;
    else return 0;
  }
};


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
// threader
////////////////////////////////////////////////////////////////////////////////////////////////

class threader : public pass
{
  struct chunk_info_t {
    char* start;
    char* end;

    chunk_info_t() : start(0) {}
  };

  pass* out;
  chunk_info_t chunks[8];
  size_t write_chunk;
  char* write_chunk_next;
  size_t read_chunk;
  bool thread_created;
  pthread_mutex_t mutex;
  pthread_cond_t prod_cond;
  pthread_cond_t cons_cond;
  pthread_t thread;

  void resize_write_chunk(size_t min_size);
  void inc_write_chunk();

public:
  friend void* threader_main(void*);

  threader();
  threader(pass& out);
  ~threader();
  threader& init();
  threader& init(pass& out);
  threader& set_out(pass& out);

  void process_token(const char* token);
  void process_line();
  void process_stream();
};


////////////////////////////////////////////////////////////////////////////////////////////////
// subset_tee
////////////////////////////////////////////////////////////////////////////////////////////////

struct subset_tee_dest_data_t
{
  std::set<std::string> key;
  std::set<std::string> key_except;
  std::vector<pcre*> regex;
  std::vector<pcre*> regex_except;
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
  subset_tee& init();
  subset_tee& init(pass& dest);
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
  std::vector<char*> data;
  char* next;
  char* end;

public:
  ordered_tee();
  ordered_tee(pass& out1, pass& out2);
  ~ordered_tee();
  ordered_tee& init();
  ordered_tee& init(pass& out1, pass& out2);
  ordered_tee& add_out(pass& out);

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
  pcre* regex;
  stack_action_e action;

  regex_stack_action_t() : regex(0) {}
  ~regex_stack_action_t() { pcre_free(regex); }
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
  stacker(stack_action_e default_action);
  stacker(pass& out, stack_action_e default_action);
  ~stacker();

  stacker& re_init();
  stacker& init(stack_action_e default_action);
  stacker& init(pass& out, stack_action_e default_action);
  stacker& set_out(pass& out);
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
  pcre* regex;
  split_action_e action;

  regex_split_action_t() : regex(0) {}
  ~regex_split_action_t() { pcre_free(regex); }
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
  char* group_tokens;
  char* group_tokens_next;
  char* group_tokens_end;
  char* split_by_tokens;
  char* split_by_tokens_next;
  char* split_by_tokens_end;
  char* split_tokens;
  char* split_tokens_next;
  char* split_tokens_end;

  std::map<char*, size_t, cstr_less> out_split_keys;
  std::vector<char*> group_storage;
  char* group_storage_next;
  char* group_storage_end;
  typedef std::tr1::unordered_map<char*, std::vector<std::string>, multi_cstr_hash, multi_cstr_equal_to> data_t;
  data_t data;

public:
  splitter(split_action_e default_action);
  splitter(pass& out, split_action_e default_action);
  splitter& init(split_action_e default_action);
  splitter& init(pass& out, split_action_e default_action);
  ~splitter();

  splitter& set_out(pass& out);
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
// substitutor
////////////////////////////////////////////////////////////////////////////////////////////////

class substitutor : public pass {
  struct sub_t
  {
    pcre* regex;
    pcre* from;
    pcre_extra* from_extra;
    std::string to;

    sub_t() : regex(0), from(0), from_extra(0) {}
    ~sub_t() { pcre_free_study(from_extra); pcre_free(from); pcre_free(regex); }
  };

  pass* out;
  std::vector<sub_t> subs;
  std::vector<pcre*> exceptions;
  std::vector<sub_t*> column_subs;
  bool first_line;
  int column;
  char* buf;
  char* end;

public:
  substitutor();
  substitutor(pass& out);
  ~substitutor();
  substitutor& init();
  substitutor& init(pass& out);
  substitutor& set_out(pass& out);
  substitutor& add(const char* regex, const char* from, const char* to);
  substitutor& add_exception(const char* regex);

  void process_token(const char* token);
  void process_line();
  void process_stream();
};


////////////////////////////////////////////////////////////////////////////////////////////////
// col_adder
////////////////////////////////////////////////////////////////////////////////////////////////

class col_adder : public pass {
  struct sub_t
  {
    pcre* regex;
    std::string new_key;
    pcre* from;
    pcre_extra* from_extra;
    std::string to;

    sub_t() : regex(0), from(0), from_extra(0) {}
    ~sub_t() { pcre_free_study(from_extra); pcre_free(from); pcre_free(regex); }
  };

  pass* out;
  std::vector<sub_t> subs;
  std::vector<pcre*> exceptions;
  std::vector<sub_t*> column_subs;
  bool first_line;
  int column;
  char* buf;
  char* end;

public:
  col_adder();
  col_adder(pass& out);
  ~col_adder();
  col_adder& init();
  col_adder& init(pass& out);
  col_adder& set_out(pass& out);
  col_adder& add(const char* regex, const char* new_key, const char* from, const char* to);
  col_adder& add_exception(const char* regex);

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
  std::vector<char*> data;
  char* next;
  char* end;

public:
  col_pruner();
  col_pruner(pass& out);
  ~col_pruner();
  col_pruner& init();
  col_pruner& init(pass& out);
  col_pruner& set_out(pass& out);

  void process_token(const char* token);
  void process_line();
  void process_stream();
};


////////////////////////////////////////////////////////////////////////////////////////////////
// combiner
////////////////////////////////////////////////////////////////////////////////////////////////

class combiner : public pass {
  pass* out;
  std::vector<std::pair<pcre*, std::string> > pairs;
  bool first_row;
  int column;
  std::vector<int> remap_indexes;
  std::vector<std::string> tokens;

public:
  combiner();
  combiner(pass& out);
  ~combiner();
  combiner& init();
  combiner& init(pass& out);
  combiner& set_out(pass& out);
  combiner& add_pair(const char* from, const char* to);

  void process_token(const char* token);
  void process_line();
  void process_stream();
};


////////////////////////////////////////////////////////////////////////////////////////////////
// writer
////////////////////////////////////////////////////////////////////////////////////////////////

class csv_writer_base : public pass {
protected:
  std::streambuf* out;
  int line;
  int num_columns;
  int column;

  csv_writer_base() : out(0) {}
  void base_init();

public:
  void process_token(const char* token);
  void process_line();
};

class csv_writer : public csv_writer_base {
public:
  csv_writer();
  csv_writer(std::streambuf* out);
  csv_writer& init();
  csv_writer& init(std::streambuf* out);
  csv_writer& set_out(std::streambuf* out);

  void process_stream();
};

class csv_file_writer : public csv_writer_base {
public:
  csv_file_writer();
  csv_file_writer(const char* filename);
  csv_file_writer& init();
  csv_file_writer& init(const char* filename);
  csv_file_writer& set_out(const char* filename);
  ~csv_file_writer();

  void process_stream();
};


////////////////////////////////////////////////////////////////////////////////////////////////
// driver
////////////////////////////////////////////////////////////////////////////////////////////////

void read_csv(std::streambuf* in, pass& out);
void read_csv(const char* filename, pass& out);


////////////////////////////////////////////////////////////////////////////////////////////////
// numeric templates
////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////
// unary_col_modifier
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename UnaryOperation> class unary_col_modifier : public pass {
  struct inst_t
  {
    pcre* regex;
    UnaryOperation unary_op;

    inst_t() : regex(0) {}
    ~inst_t() { pcre_free(regex); }
  };

  pass* out;
  std::vector<inst_t> insts;
  bool first_row;
  int column;
  std::vector<UnaryOperation*> column_insts;

public:
  unary_col_modifier();
  unary_col_modifier(pass& out);
  unary_col_modifier& init();
  unary_col_modifier& init(pass& out);
  unary_col_modifier& set_out(pass& out);
  unary_col_modifier& add(const char* regex, const UnaryOperation& unary_op);

  void process_token(const char* token);
  void process_line();
  void process_stream();
};


////////////////////////////////////////////////////////////////////////////////////////////////
// unary_col_adder
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename UnaryOperation> class unary_col_adder : public pass {
  struct inst_t
  {
    pcre* regex;
    std::string new_key;
    UnaryOperation unary_op;

    inst_t() : regex(0) {}
    ~inst_t() { pcre_free(regex); }
  };

  struct col_t
  {
    size_t col;
    double val;
    UnaryOperation* unary_op;
    std::string new_key;
  };

  pass* out;
  std::vector<inst_t> insts;
  bool first_row;
  size_t column;
  char* buf;
  char* end;
  std::vector<col_t> columns;
  typename std::vector<col_t>::iterator ci;

public:
  unary_col_adder();
  unary_col_adder(pass& out);
  unary_col_adder& init();
  unary_col_adder& init(pass& out);
  unary_col_adder& set_out(pass& out);
  unary_col_adder& add(const char* regex, const char* new_key, const UnaryOperation& unary_op);

  void process_token(const char* token);
  void process_line();
  void process_stream();
};


////////////////////////////////////////////////////////////////////////////////////////////////
// binary_col_modifier
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename BinaryOperation> class binary_col_modifier : public pass {
  struct inst_t
  {
    pcre* regex;
    std::string other_key;
    BinaryOperation binary_op;

    inst_t() : regex(0) {}
    ~inst_t() { pcre_free(regex); }
  };

  struct col_info_t { size_t index; bool passthrough; };
  struct new_col_info_t { size_t other_col; BinaryOperation* binary_op; };
  struct col_t { size_t col; double val; bool passthrough; };
  struct new_col_t { size_t col_index; size_t other_col_index; BinaryOperation* binary_op; };

  pass* out;
  std::vector<inst_t> insts;
  bool first_row;
  size_t column;
  std::vector<char*> key_storage;
  char* key_storage_next;
  char* key_storage_end;
  std::vector<char*> keys;
  std::vector<col_t> columns;
  typename std::vector<col_t>::iterator ci;
  std::vector<new_col_t> new_columns;

public:
  binary_col_modifier();
  binary_col_modifier(pass& out);
  ~binary_col_modifier();
  binary_col_modifier& init();
  binary_col_modifier& init(pass& out);
  binary_col_modifier& set_out(pass& out);
  binary_col_modifier& add(const char* regex, const char* other_key, const BinaryOperation& binary_op);

  void process_token(const char* token);
  void process_line();
  void process_stream();
};


////////////////////////////////////////////////////////////////////////////////////////////////
// numeric.cpp
////////////////////////////////////////////////////////////////////////////////////////////////

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
  std::vector<pcre*> group_regexes;
  std::vector<std::pair<pcre*, uint32_t> > data_regexes;
  std::vector<pcre*> exception_regexes;

  bool first_line;
  std::vector<uint32_t> column_flags;
  size_t num_data_columns;

  std::vector<uint32_t>::const_iterator cfi;
  double* values;
  double* vi;
  char* group_tokens;
  char* group_tokens_next;
  char* group_tokens_end;

  std::vector<char*> group_storage;
  char* group_storage_next;
  char* group_storage_end;
  std::vector<summarizer_data_t*> data_storage;
  summarizer_data_t* data_storage_next;
  summarizer_data_t* data_storage_end;
  typedef std::tr1::unordered_map<char*, summarizer_data_t*, multi_cstr_hash, multi_cstr_equal_to> data_t;
  data_t data;

public:
  summarizer();
  summarizer(pass& out);
  summarizer& init();
  summarizer& set_out(pass& out);
  summarizer& init(pass& out);
  ~summarizer();

  summarizer& add_group(const char* regex);
  summarizer& add_data(const char* regex, uint32_t flags);
  summarizer& add_exception(const char* regex);

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
    pcre* regex;
    int from;
    int to;

    regex_base_conv_t() : regex(0) {}
    ~regex_base_conv_t() { pcre_free(regex); }
  };

  struct conv_t { int from; int to; };

  pass* out;
  std::vector<regex_base_conv_t> regex_base_conv;
  bool first_row;
  int column;
  std::vector<conv_t> conv;

public:
  base_converter();
  base_converter(pass& out, const char* regex, int from, int to);
  ~base_converter();
  base_converter& init();
  base_converter& init(pass& out, const char* regex, int from, int to);
  base_converter& set_out(pass& out);
  base_converter& add_conv(const char* regex, int from, int to);

  void process_token(const char* token);
  void process_line();
  void process_stream();
};


////////////////////////////////////////////////////////////////////////////////////////////////
// variance_analyzer
////////////////////////////////////////////////////////////////////////////////////////////////

struct variance_analyzer_treatment_data_t
{
  int count;
  double sum;
  double sum_of_squares;

  variance_analyzer_treatment_data_t() : count(0), sum(0.0), sum_of_squares(0.0) {}
};

class variance_analyzer : public pass {
  pass* out;
  std::vector<pcre*> group_regexes;
  std::vector<pcre*> data_regexes;
  std::vector<pcre*> exception_regexes;

  bool first_line;
  std::vector<char> column_type; // 0 = ignore, 1 = group, 2 = data
  std::vector<std::string> data_keywords;

  std::vector<char>::const_iterator cti;
  char* group_tokens;
  char* group_tokens_next;
  char* group_tokens_end;
  std::vector<char*> group_storage;
  char* group_storage_next;
  char* group_storage_end;
  typedef std::map<char*, size_t, multi_cstr_less> groups_t;
  groups_t groups;
  double* values;
  double* vi;
  typedef std::vector<variance_analyzer_treatment_data_t*> data_t;
  data_t data; // keyword fast, group/treatment slow

public:
  variance_analyzer();
  variance_analyzer(pass& out);
  ~variance_analyzer();
  variance_analyzer& init();
  variance_analyzer& init(pass& out);
  variance_analyzer& set_out(pass& out);

  variance_analyzer& add_group(const char* regex);
  variance_analyzer& add_data(const char* regex);
  variance_analyzer& add_exception(const char* regex);

  void process_token(const char* token);
  void process_line();
  void process_stream();
};


}


#include "numeric_imp.h"

#endif


