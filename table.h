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

    threader(const threader& other);
    threader& operator=(const threader& other);
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

    subset_tee(const subset_tee& other);
    subset_tee& operator=(const subset_tee& other);

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

    ordered_tee(const ordered_tee& other);
    ordered_tee& operator=(const ordered_tee& other);

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

    stacker(const stacker& other);
    stacker& operator=(const stacker& other);
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

    splitter(const splitter& other);
    splitter& operator=(const splitter& other);

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

    row_joiner(const row_joiner& other);
    row_joiner& operator=(const row_joiner& other);

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

    substitutor(const substitutor& other);
    substitutor& operator=(const substitutor& other);

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

    col_adder(const col_adder& other);
    col_adder& operator=(const col_adder& other);

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

    col_pruner(const col_pruner& other);
    col_pruner& operator=(const col_pruner& other);

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

    combiner(const combiner& other);
    combiner& operator=(const combiner& other);

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

    tabular_writer(const tabular_writer& other);
    tabular_writer& operator=(const tabular_writer& other);
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
    csv_writer(const csv_writer& other);
    csv_writer& operator=(const csv_writer& other);

  public:
    csv_writer();
    csv_writer(std::streambuf* out);
    csv_writer& init();
    csv_writer& init(std::streambuf* out);
    csv_writer& set_out(std::streambuf* out);

    void process_stream();
  };

  class csv_file_writer : public csv_writer_base {
    csv_file_writer(const csv_file_writer& other);
    csv_file_writer& operator=(const csv_file_writer& other);

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

    basic_unary_col_modifier(const basic_unary_col_modifier& other);
    basic_unary_col_modifier& operator=(const basic_unary_col_modifier& other);

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

    basic_unary_col_adder(const basic_unary_col_adder& other);
    basic_unary_col_adder& operator=(const basic_unary_col_adder& other);

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

    basic_binary_col_modifier(const basic_binary_col_modifier& other);
    basic_binary_col_modifier& operator=(const basic_binary_col_modifier& other);

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

    summarizer(const summarizer& other);
    summarizer& operator=(const summarizer& other);

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

    differ(const differ& other);
    differ& operator=(const differ& other);

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

    base_converter(const base_converter& other);
    base_converter& operator=(const base_converter& other);

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

    variance_analyzer(const variance_analyzer& other);
    variance_analyzer& operator=(const variance_analyzer& other);

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


////////////////////////////////////////////////////////////////////////////////////////////////
// implementation stuff that can change with TABLE_MINOR
////////////////////////////////////////////////////////////////////////////////////////////////

#include <stdexcept>
#include <sstream>
#include <limits>
#include <math.h>
#include <stdio.h>


namespace table {

  using namespace std;

  ////////////////////////////////////////////////////////////////////////////////////////////////
  // unary_col_modifier
  ////////////////////////////////////////////////////////////////////////////////////////////////

  template<typename UnaryOperation> basic_unary_col_modifier<UnaryOperation>::basic_unary_col_modifier() { init(); }
  template<typename UnaryOperation> basic_unary_col_modifier<UnaryOperation>::basic_unary_col_modifier(pass& out) { init(out); }

  template<typename UnaryOperation> basic_unary_col_modifier<UnaryOperation>& basic_unary_col_modifier<UnaryOperation>::init()
  {
    out = 0;
    insts.clear();
    first_row = 1;
    column = 0;
    column_insts.clear();
    return *this;
  }

  template<typename UnaryOperation> basic_unary_col_modifier<UnaryOperation>& basic_unary_col_modifier<UnaryOperation>::init(pass& out) { init(); return set_out(out); }
  template<typename UnaryOperation> basic_unary_col_modifier<UnaryOperation>& basic_unary_col_modifier<UnaryOperation>::set_out(pass& out) { this->out = &out; return *this; }

  template<typename UnaryOperation> basic_unary_col_modifier<UnaryOperation>& basic_unary_col_modifier<UnaryOperation>::add(const char* regex, const UnaryOperation& unary_op)
  {
    const char* err; int err_off; pcre* p = pcre_compile(regex, 0, &err, &err_off, 0);
    if(!p) throw runtime_error("basic_unary_col_modifier can't compile regex");
    insts.resize(insts.size() + 1);
    insts.back().regex = p;
    insts.back().unary_op = unary_op;
    return *this;
  }

  template<typename UnaryOperation> void basic_unary_col_modifier<UnaryOperation>::process_token(const char* token, size_t len)
  {
    if(first_row) {
      if(!out) throw runtime_error("basic_unary_col_modifier has no out");
      UnaryOperation* p = 0;
      for(typename vector<inst_t>::iterator i = insts.begin(); i != insts.end(); ++i) {
        int ovector[30]; int rc = pcre_exec((*i).regex, 0, token, len, 0, 0, ovector, 30);
        if(rc >= 0) { p = &(*i).unary_op; break; }
        else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("basic_unary_col_modifier match error");
      }
      column_insts.push_back(p);
      out->process_token(token, len);
    }
    else {
      if(!column_insts[column]) out->process_token(token, len);
      else {
        char* next; double val = strtod(token, &next);
        if(next == token) val = numeric_limits<double>::quiet_NaN();
        out->process_token((*column_insts[column])(val));
      }
    }

    ++column;
  }

  template<typename UnaryOperation> void basic_unary_col_modifier<UnaryOperation>::process_token(double token)
  {
    if(first_row) { char buf[32]; size_t len = dtostr(token, buf); process_token(buf, len); return; }

    if(!column_insts[column]) out->process_token(token);
    else out->process_token((*column_insts[column])(token));

    ++column;
  }

  template<typename UnaryOperation> void basic_unary_col_modifier<UnaryOperation>::process_line()
  {
    if(!out) throw runtime_error("basic_unary_col_modifier has no out");
    out->process_line();
    first_row = 0;
    column = 0;
  }

  template<typename UnaryOperation> void basic_unary_col_modifier<UnaryOperation>::process_stream()
  {
    if(!out) throw runtime_error("basic_unary_col_modifier has no out");
    out->process_stream();
  }


  ////////////////////////////////////////////////////////////////////////////////////////////////
  // unary_col_adder
  ////////////////////////////////////////////////////////////////////////////////////////////////

  template<typename UnaryOperation> basic_unary_col_adder<UnaryOperation>::basic_unary_col_adder() : buf(0) { init(); }
  template<typename UnaryOperation> basic_unary_col_adder<UnaryOperation>::basic_unary_col_adder(pass& out) : buf(0) { init(out); }
  template<typename UnaryOperation> basic_unary_col_adder<UnaryOperation>::~basic_unary_col_adder() { delete[] buf; }

  template<typename UnaryOperation> basic_unary_col_adder<UnaryOperation>& basic_unary_col_adder<UnaryOperation>::init()
  {
    out = 0;
    insts.clear();
    first_row = 1;
    column = 0;
    delete[] buf; buf = new char[2048];
    end = buf + 2048;
    columns.clear();
    return *this;
  }

  template<typename UnaryOperation> basic_unary_col_adder<UnaryOperation>& basic_unary_col_adder<UnaryOperation>::init(pass& out) { init(); return set_out(out); }
  template<typename UnaryOperation> basic_unary_col_adder<UnaryOperation>& basic_unary_col_adder<UnaryOperation>::set_out(pass& out) { this->out = &out; return *this; }

  template<typename UnaryOperation> basic_unary_col_adder<UnaryOperation>& basic_unary_col_adder<UnaryOperation>::add(const char* regex, const char* new_key, const UnaryOperation& unary_op)
  {
    const char* err; int err_off; pcre* p = pcre_compile(regex, 0, &err, &err_off, 0);
    if(!p) throw runtime_error("basic_unary_col_adder can't compile regex");
    insts.resize(insts.size() + 1);
    insts.back().regex = p;
    insts.back().new_key = new_key;
    insts.back().unary_op = unary_op;
    return *this;
  }

  template<typename UnaryOperation> void basic_unary_col_adder<UnaryOperation>::process_token(const char* token, size_t len)
  {
    if(first_row) {
      if(!out) throw runtime_error("basic_unary_col_adder has no out");
      for(typename vector<inst_t>::iterator i = insts.begin(); i != insts.end(); ++i) {
        int ovector[30]; int rc = pcre_exec((*i).regex, 0, token, len, 0, 0, ovector, 30);
        if(rc < 0) { if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("basic_unary_col_adder match error"); }
        else {
          columns.resize(columns.size() + 1);
          columns.back().col = column;
          columns.back().unary_op = &(*i).unary_op;
          char* next = buf;
          if(!rc) rc = 10;
          generate_substitution(token, (*i).new_key.c_str(), ovector, rc, buf, next, end);
          columns.back().new_key = buf;
        }
      }
      out->process_token(token, len);
    }
    else {
      if(ci != columns.end() && column == (*ci).col) {
        char* next; double val = strtod(token, &next);
        if(next == token) val = numeric_limits<double>::quiet_NaN();
        do { (*ci++).val = val; } while(ci != columns.end() && column == (*ci).col);
      }
      out->process_token(token, len);
    }

    ++column;
  }

  template<typename UnaryOperation> void basic_unary_col_adder<UnaryOperation>::process_token(double token)
  {
    if(first_row) { char buf[32]; size_t len = dtostr(token, buf); process_token(buf, len); return; }

    if(ci != columns.end() && column == (*ci).col) {
      do { (*ci++).val = token; } while(ci != columns.end() && column == (*ci).col);
    }
    out->process_token(token);

    ++column;
  }

  template<typename UnaryOperation> void basic_unary_col_adder<UnaryOperation>::process_line()
  {
    if(first_row) {
      if(!out) throw runtime_error("basic_unary_col_adder has no out");
      first_row = 0;
      for(typename vector<col_t>::iterator i = columns.begin(); i != columns.end(); ++i) {
        out->process_token((*i).new_key.c_str(), (*i).new_key.size());
        (*i).new_key.clear();
      }
    }
    else {
      for(typename vector<col_t>::iterator i = columns.begin(); i != columns.end(); ++i)
        out->process_token((*(*i).unary_op)((*i).val));
    }
    out->process_line();
    column = 0;
    ci = columns.begin();
  }

  template<typename UnaryOperation> void basic_unary_col_adder<UnaryOperation>::process_stream()
  {
    if(!out) throw runtime_error("basic_unary_col_adder has no out");
    out->process_stream();
  }


  ////////////////////////////////////////////////////////////////////////////////////////////////
  // binary_col_modifier
  ////////////////////////////////////////////////////////////////////////////////////////////////

  template<typename BinaryOperation> basic_binary_col_modifier<BinaryOperation>::basic_binary_col_modifier() { init(); }
  template<typename BinaryOperation> basic_binary_col_modifier<BinaryOperation>::basic_binary_col_modifier(pass& out) { init(out); }

  template<typename BinaryOperation> basic_binary_col_modifier<BinaryOperation>::~basic_binary_col_modifier()
  {
    for(vector<char*>::const_iterator i = key_storage.begin(); i != key_storage.end(); ++i) delete[] *i;
  }

  template<typename BinaryOperation> basic_binary_col_modifier<BinaryOperation>& basic_binary_col_modifier<BinaryOperation>::init()
  {
    out = 0;
    insts.clear();
    first_row = 1;
    column = 0;
    for(vector<char*>::const_iterator i = key_storage.begin(); i != key_storage.end(); ++i) delete[] *i;
    key_storage.clear();
    key_storage_next = 0;
    key_storage_end = 0;
    keys.clear();
    columns.clear();
    new_columns.clear();
    return *this;
  }

  template<typename BinaryOperation> basic_binary_col_modifier<BinaryOperation>& basic_binary_col_modifier<BinaryOperation>::init(pass& out) { init(); return set_out(out); }
  template<typename BinaryOperation> basic_binary_col_modifier<BinaryOperation>& basic_binary_col_modifier<BinaryOperation>::set_out(pass& out) { this->out = &out; return *this; }

  template<typename BinaryOperation> basic_binary_col_modifier<BinaryOperation>& basic_binary_col_modifier<BinaryOperation>::add(const char* regex, const char* other_key, const BinaryOperation& binary_op)
  {
    const char* err; int err_off; pcre* p = pcre_compile(regex, 0, &err, &err_off, 0);
    if(!p) throw runtime_error("basic_binary_col_modifier can't compile regex");
    insts.resize(insts.size() + 1);
    insts.back().regex = p;
    insts.back().other_key = other_key;
    insts.back().binary_op = binary_op;
    return *this;
  }

  template<typename BinaryOperation> void basic_binary_col_modifier<BinaryOperation>::process_token(const char* token, size_t len)
  {
    if(first_row) {
      if(!out) throw runtime_error("basic_binary_col_modifier has no out");
      if(len + 1 > size_t(key_storage_end - key_storage_next)) {
        size_t cap = 256 * 1024;
        if(cap < len + 1) cap = len + 1;
        key_storage.push_back(new char[cap]);
        key_storage_next = key_storage.back();
        key_storage_end = key_storage.back() + cap;
      }
      keys.push_back(key_storage_next);
      memcpy(key_storage_next, token, len); key_storage_next += len;
      *key_storage_next++ = '\0';
    }
    else {
      if(ci == columns.end() || (*ci).col != column) out->process_token(token, len);
      else {
        char* next; (*ci).val = strtod(token, &next);
        if(next == token) (*ci).val = numeric_limits<double>::quiet_NaN();

        if((*ci).passthrough) out->process_token(token, len);

        ++ci;
      }
    }

    ++column;
  }

  template<typename BinaryOperation> void basic_binary_col_modifier<BinaryOperation>::process_token(double token)
  {
    if(first_row) { char buf[32]; size_t len = dtostr(token, buf); process_token(buf, len); return; }

    if(ci == columns.end() || (*ci).col != column) out->process_token(token);
    else {
      (*ci).val = token;
      if((*ci).passthrough) out->process_token(token);
      ++ci;
    }

    ++column;
  }

  template<typename BinaryOperation> void basic_binary_col_modifier<BinaryOperation>::process_line()
  {
    if(first_row) {
      if(!out) throw runtime_error("basic_binary_col_modifier has no out");

      char* buf = new char[2048];
      char* end = buf + 2048;
      vector<size_t> lens;
      map<size_t, col_info_t> cols;
      map<size_t, new_col_info_t> new_cols;

      for(column = 0; column < keys.size(); ++column) {
        const size_t len = strlen(keys[column]);
        lens.push_back(len);
        for(typename vector<inst_t>::iterator ii = insts.begin(); ii != insts.end(); ++ii) {
          int ovector[30]; int rc = pcre_exec((*ii).regex, 0, keys[column], len, 0, 0, ovector, 30);
          if(rc < 0){ if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("basic_binary_col_modifier match error"); }
          else {
            char* next = buf;
            if(!rc) rc = 10;
            generate_substitution(keys[column], (*ii).other_key.c_str(), ovector, rc, buf, next, end);
            size_t other_col = 0;
            while(other_col < keys.size() && strcmp(keys[other_col], buf)) ++other_col;
            if(other_col >= keys.size()) {
              stringstream msg; msg << "basic_binary_col_modifier could not find " << buf << " which is paired with " << keys[column];
              throw runtime_error(msg.str());
            }
            cols[column].passthrough = 0;
            if(cols.end() == cols.find(other_col)) cols[other_col].passthrough = 1;
            new_col_info_t& nci = new_cols[column];
            nci.other_col = other_col;
            nci.binary_op = &(*ii).binary_op;
            break;
          }
        }
      }

      for(column = 0; column < keys.size(); ++column) {
        typename map<size_t, col_info_t>::iterator i = cols.find(column);
        if(i == cols.end() || (*i).second.passthrough)
          out->process_token(keys[column], lens[column]);
      }

      for(typename map<size_t, col_info_t>::iterator i = cols.begin(); i != cols.end(); ++i) {
        (*i).second.index = columns.size();
        col_t c;
        c.col = (*i).first;
        c.passthrough = (*i).second.passthrough;
        columns.push_back(c);
      }

      for(typename map<size_t, new_col_info_t>::iterator i = new_cols.begin(); i != new_cols.end(); ++i) {
        new_col_t c;
        c.col_index = cols[(*i).first].index;
        c.other_col_index = cols[(*i).second.other_col].index;
        c.binary_op = (*i).second.binary_op;
        new_columns.push_back(c);
        out->process_token(keys[(*i).first], lens[(*i).first]);
      }

      delete[] buf;
      first_row = 0;
      keys.clear();
      for(vector<char*>::const_iterator i = key_storage.begin(); i != key_storage.end(); ++i) delete[] *i;
      key_storage.clear();
      key_storage_next = 0;
      key_storage_end = 0;
    }
    else {
      for(typename vector<new_col_t>::iterator i = new_columns.begin(); i != new_columns.end(); ++i)
        out->process_token((*(*i).binary_op)(columns[(*i).col_index].val, columns[(*i).other_col_index].val));
    }
    out->process_line();
    column = 0;
    ci = columns.begin();
  }

  template<typename BinaryOperation> void basic_binary_col_modifier<BinaryOperation>::process_stream()
  {
    if(!out) throw runtime_error("basic_binary_col_modifier has no out");
    out->process_stream();
  }
}


#endif


