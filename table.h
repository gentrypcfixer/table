#ifndef table_h_
#define table_h_

#define TABLE_MAJOR @TABLE_MAJOR@
#define TABLE_MINOR @TABLE_MINOR@

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

void resize_buffer(char*& buf, char*& next, char*& end, size_t min_to_add = 0, char** resize_end = 0);
void generate_substitution(const char* token, const char* replace_with, const int* ovector, int num_captured, char*& buf, char*& next, char*& end);
int dtostr(double value, char* str, int prec = 6);

struct cstr_less {
  bool operator()(char* const& lhs, char* const& rhs) const { return 0 > strcmp(lhs, rhs); }
  bool operator()(const char* const& lhs, const char* const& rhs) const { return 0 > strcmp(lhs, rhs); }
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
// pass
////////////////////////////////////////////////////////////////////////////////////////////////

class pass {
public:
  virtual ~pass();

  virtual void process_token(const char* token, size_t len) = 0;
  virtual void process_token(double token);
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
  void inc_write_chunk(bool term = 1);

public:
  friend void* threader_main(void*);

  threader();
  threader(pass& out);
  ~threader();
  threader& init();
  threader& init(pass& out);
  threader& set_out(pass& out);

  void process_token(const char* token, size_t len);
  void process_token(double token);
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

  void process_token(const char* token, size_t len);
  void process_token(double token);
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

  void process_token(const char* token, size_t len);
  void process_token(double token);
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

class stacker : public pass {
  struct regex_stack_action_t
  {
    pcre* regex;
    stack_action_e action;
  };

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
  std::vector<std::pair<char*, char*> > leave_tokens;
  size_t leave_tokens_index;
  char* leave_tokens_next;
  std::vector<std::pair<char*, char*> > stack_tokens;
  size_t stack_tokens_index;
  char* stack_tokens_next;

  void push(const char* token, size_t len, std::vector<std::pair<char*, char*> >& tokens, size_t& index, char*& next);
  void process_out_line(const char* token, size_t len);

public:
  stacker(stack_action_e default_action);
  stacker(pass& out, stack_action_e default_action);
  ~stacker();

  stacker& re_init();
  stacker& init(stack_action_e default_action);
  stacker& init(pass& out, stack_action_e default_action);
  stacker& set_out(pass& out);
  stacker& add_action(bool regex, const char* key, stack_action_e action);

  void process_token(const char* token, size_t len);
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

  void process_token(const char* token, size_t len);
  void process_line();
  void process_stream();
};


////////////////////////////////////////////////////////////////////////////////////////////////
// row_joiner
////////////////////////////////////////////////////////////////////////////////////////////////

class row_joiner : public pass {
  struct data_t {
    std::vector<char*> data;
    std::vector<char*>::iterator i;
    char* next;
    char* end;
  };

  pass* out;
  std::vector<std::string> table_name;
  size_t table;

  bool first_line;
  bool more_lines;
  size_t column;
  std::vector<size_t> num_columns;
  std::vector<std::string> keys;

  std::vector<data_t> data;

public:
  row_joiner();
  row_joiner(pass& out, const char* table_name = 0);
  ~row_joiner();
  row_joiner& init(pass& out, const char* table_name = 0);
  row_joiner& add_table_name(const char* name = 0);

  void process_token(const char* token, size_t len);
  void process_token(double token);
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

  void process_token(const char* token, size_t len);
  void process_token(double token);
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

  void process_token(const char* token, size_t len);
  void process_token(double token);
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

  void process_token(const char* token, size_t len);
  void process_token(double token);
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

  void process_token(const char* token, size_t len);
  void process_line();
  void process_stream();
};


////////////////////////////////////////////////////////////////////////////////////////////////
// tabular_writer
////////////////////////////////////////////////////////////////////////////////////////////////

class tabular_writer : public table::pass {
  std::streambuf* out;
  int line;
  size_t column;
  std::vector<size_t> max_width;
  std::vector<char*> data;
  char* next;
  char* end;

  void process_data();

public:
  tabular_writer();
  tabular_writer(std::streambuf* out);
  tabular_writer& init();
  tabular_writer& init(std::streambuf* out);
  tabular_writer& set_out(std::streambuf* out);

  void process_token(const char* token, size_t len);
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
  void process_token(const char* token, size_t len);
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

template<typename UnaryOperation> class basic_unary_col_modifier : public pass {
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
  basic_unary_col_modifier();
  basic_unary_col_modifier(pass& out);
  basic_unary_col_modifier& init();
  basic_unary_col_modifier& init(pass& out);
  basic_unary_col_modifier& set_out(pass& out);
  basic_unary_col_modifier& add(const char* regex, const UnaryOperation& unary_op);

  void process_token(const char* token, size_t len);
  void process_token(double token);
  void process_line();
  void process_stream();
};

typedef basic_unary_col_modifier<double (*)(double)> unary_col_modifier;


////////////////////////////////////////////////////////////////////////////////////////////////
// unary_col_adder
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename UnaryOperation> class basic_unary_col_adder : public pass {
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
  basic_unary_col_adder();
  basic_unary_col_adder(pass& out);
  ~basic_unary_col_adder();
  basic_unary_col_adder& init();
  basic_unary_col_adder& init(pass& out);
  basic_unary_col_adder& set_out(pass& out);
  basic_unary_col_adder& add(const char* regex, const char* new_key, const UnaryOperation& unary_op);

  void process_token(const char* token, size_t len);
  void process_token(double token);
  void process_line();
  void process_stream();
};

typedef basic_unary_col_adder<double (*)(double)> unary_col_adder;


////////////////////////////////////////////////////////////////////////////////////////////////
// binary_col_modifier
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename BinaryOperation> class basic_binary_col_modifier : public pass {
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
  basic_binary_col_modifier();
  basic_binary_col_modifier(pass& out);
  ~basic_binary_col_modifier();
  basic_binary_col_modifier& init();
  basic_binary_col_modifier& init(pass& out);
  basic_binary_col_modifier& set_out(pass& out);
  basic_binary_col_modifier& add(const char* regex, const char* other_key, const BinaryOperation& binary_op);

  void process_token(const char* token, size_t len);
  void process_token(double token);
  void process_line();
  void process_stream();
};

typedef basic_binary_col_modifier<double (*)(double, double)> binary_col_modifier;


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

  void process_token(const char* token, size_t len);
  void process_token(double token);
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

  void process_token(const char* token, size_t len);
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

  void process_token(const char* token, size_t len);
  void process_token(double token);
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

  void process_token(const char* token, size_t len);
  void process_token(double token);
  void process_line();
  void process_stream();
};


}


#include "numeric_imp.h"

#endif


