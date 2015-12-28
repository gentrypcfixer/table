#ifndef table_h_
#define table_h_

#define TABLE_MAJOR_VER 4
#define TABLE_MINOR_VER 0

#include <string>
#include <sstream>
#include <fstream>
//#include <tr1/unordered_map>
#include <limits>
#include <algorithm>
#include <map>
#include <set>
#include <stack>
#include <vector>
#include <pcre.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <stdexcept>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#ifdef _WIN32
#include <windows.h>
#endif


#if(PCRE_MAJOR < 8 || (PCRE_MAJOR == 8 && PCRE_MINOR < 20))
#define PCRE_STUDY_JIT_COMPILE 0
inline void pcre_free_study(void *p) { free(p); }
#endif


namespace table {

using namespace std;

int major_ver();
int minor_ver();

extern void resize_buffer(char*& buf, char*& next, char*& end, size_t min_to_add = 0, char** resize_end = 0);
extern void generate_substitution(const char* token, const char* replace_with, const int* ovector, int num_captured, char*& buf, char*& next, char*& end);
extern int dtostr(double value, char* str, int prec = 6);
extern float ibeta(float a, float b, float x);

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

struct c_str_and_len_t
{
  const char* c_str;
  size_t len;
  c_str_and_len_t() : c_str(0), len(0) {}
  c_str_and_len_t(const char* c_str, size_t len) : c_str(c_str), len(len) {}
};

class empty_class_t {};


////////////////////////////////////////////////////////////////////////////////////////////////
// setting_fetcher
////////////////////////////////////////////////////////////////////////////////////////////////

const int st_key = 0x1;
const int st_val = 0x2;

class setting_fetcher {
protected:
  int cur_type;
  const char* key_;
  size_t key_len_;
  const char* val_;
  size_t val_len_;
  setting_fetcher() : cur_type(0) {}

public:
  int type() { return cur_type; }
  const char* key() { return key_; }
  size_t key_len() { return key_len_; }
  const char* val() { return val_; }
  size_t val_len() { return val_len_; }
};

const int st_dash = 0x4;
const int st_ddash = 0x8;
const int st_plus = 0x10;
const int st_first_split_val = 0x20;
const int st_more_split_vals = 0x40;

class arg_fetcher : public setting_fetcher {
  int argc;
  const char** argv;
  bool (*split_csv_values)(int, const char*, size_t);

  int arg;
  const char* argp;
  int state;
  char* buf;
  char* next;
  char* end;
  stack<filebuf*> files;

public:
  arg_fetcher(int argc, char** argv, bool (*split_csv_values)(int, const char*, size_t) = 0) : argc(argc), argv((const char**)argv), split_csv_values(split_csv_values), arg(0), argp(argc ? argv[0] : 0), buf(0) { get_next(); }
  arg_fetcher(int argc, const char** argv, bool (*split_csv_values)(int, const char*, size_t) = 0) : argc(argc), argv(argv), split_csv_values(split_csv_values), arg(0), argp(argc ? argv[0] : 0), buf(0) { get_next(); }
  arg_fetcher(const char* arg, bool (*split_csv_values)(int, const char*, size_t) = 0) : argc(0), argv(0), split_csv_values(split_csv_values), arg(0), argp(arg), buf(0) { get_next(); }
  ~arg_fetcher() { while(files.size()) { delete files.top(); files.pop(); } }

  void rewind() { while(files.size()) { delete files.top(); files.pop(); } arg = 0; argp = argc ? argv[0] : 0; get_next(); }
  void get_next();
};

extern bool always_split_arg(int type, const char* key, size_t len);


////////////////////////////////////////////////////////////////////////////////////////////////
// pass
////////////////////////////////////////////////////////////////////////////////////////////////

class dynamic_pass_t {
public:
  virtual ~dynamic_pass_t() {}
  virtual void reinit(int more_passes = 0) = 0;
  virtual void reinit_state(int more_passes = 0) = 0;
  virtual void process_token(const char* token, size_t len) = 0;
  virtual void process_token(double token) = 0;
  virtual void process_line() = 0;
  virtual void process_stream() = 0;
};


////////////////////////////////////////////////////////////////////////////////////////////////
// classes that output to a pass
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename out_t> class single_output_pass_class_t {
protected:
  out_t out;

  void reinit_output_if(int more_passes = 0) { if(more_passes > 0) out.reinit(--more_passes); else if(more_passes < 0) out.reinit(more_passes); }
  void reinit_output_state_if(int more_passes = 0) { if(more_passes > 0) out.reinit_state(--more_passes); else if(more_passes < 0) out.reinit_state(more_passes); }
  void output_token(const char* token, size_t len) { out.process_token(token, len); }
  void output_token(double token) { out.process_token(token); }
  void output_line() { out.process_line(); }
  void output_stream() { out.process_stream(); }

public:
  out_t& get_out() { return out; }
};

template<> class single_output_pass_class_t<dynamic_pass_t*> {
protected:
  dynamic_pass_t* out;

  void reinit_output_if(int more_passes = 0) { if(more_passes > 0) out->reinit(--more_passes); else if(more_passes < 0) out->reinit(more_passes); }
  void reinit_output_state_if(int more_passes = 0) { if(more_passes > 0) out->reinit_state(--more_passes); else if(more_passes < 0) out->reinit_state(more_passes); }
  void output_token(const char* token, size_t len) { out->process_token(token, len); }
  void output_token(double token) { out->process_token(token); }
  void output_line() { out->process_line(); }
  void output_stream() { out->process_stream(); }

public:
  single_output_pass_class_t() : out(0) {}
  virtual ~single_output_pass_class_t() {}
  dynamic_pass_t* get_out() { return out; }
  void set_out(dynamic_pass_t* out) { if(!out) throw runtime_error("null out"); this->out = out; }
};


////////////////////////////////////////////////////////////////////////////////////////////////
// writer
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename out_t> class buffered_writer_t : public out_t
{
  buffered_writer_t(const buffered_writer_t& other);
  buffered_writer_t& operator=(const buffered_writer_t& other);

protected:
  static const size_t buf_cap = 32 * 1024;
  char buf[buf_cap];
  char* next;
  char* end;

public:
  buffered_writer_t() : next(buf), end(buf + buf_cap) {}
  ~buffered_writer_t() { if(next != buf) out_t::write(buf, next - buf, 1); }
  void write(char c) { *next++ = c; if(next == end) { out_t::write(buf, next - buf); next = buf; } }
  void write(const char* token, size_t len) {
    for(const char* tbegin = token; len;) {
      if(next == buf && len >= buf_cap) { out_t::write(token, buf_cap); tbegin += buf_cap; len -= buf_cap; continue; }
      const size_t rem = end - next;
      size_t l = len > rem ? rem : len;
      memcpy(next, tbegin, l); next += l;
      if(next == end) { out_t::write(buf, next - buf); next = buf; tbegin += l; len -= l; }
      else break;
    }
  }
  void write(double token) {
    if(next + 32 < end) { size_t len = dtostr(token, next); next += len; }
    else { char buf[32]; size_t len = dtostr(token, buf); write(buf, len); }
  }
  void flush() { if(next != buf) { out_t::write(buf, next - buf); next = buf; } }
};

template<typename out_t> class writer_pass_t
{
protected:
  buffered_writer_t<out_t> out;
  void output(char c) { out.write(c); }
  void output(const char* token, size_t len) { out.write(token, len); }
  void output(double token) { out.write(token); }
  void flush() { out.flush(); }
};

class console_writer_t
{
  console_writer_t(const console_writer_t& other);
  console_writer_t& operator=(const console_writer_t& other);
#ifdef _WIN32
  HANDLE h;
#else
  int fd;
#endif

public:
#ifdef _WIN32
  console_writer_t() : h(INVALID_HANDLE_VALUE) {}
  void set_fd(int fd) {
    if(fd == STDIN_FILENO) h = GetStdHandle(STD_INPUT_HANDLE);
    else if(fd == STDOUT_FILENO) h = GetStdHandle(STD_OUTPUT_HANDLE);
    else if(fd == STDERR_FILENO) h = GetStdHandle(STD_ERROR_HANDLE);
    else throw runtime_error("invalid fd");
  }
  void write(const void* buf, size_t len, bool silent = 0) {
    DWORD num_written; BOOL success = WriteFile(h, buf, len, &num_written, NULL);
    if(!silent && (!success || num_written != len)) { throw runtime_error("unable to write"); }
  }
#else
  console_writer_t() : fd(-1) {}
  void set_fd(int fd) { if(fd < 0) throw runtime_error("invalid fd"); this->fd = fd; }
  void write(const void* buf, size_t len, bool silent = 0) {
    for(const char* begin = static_cast<const char*>(buf); len;) {
      ssize_t num_written = ::write(fd, begin, len);
      if(num_written == (ssize_t)len) { break; }
      else if(num_written >= 0) { begin += num_written; len -= num_written; }
      else if(errno == EINTR) {}
      else if(!silent) { throw runtime_error("unable to write"); }
    }
  }
#endif
};

class console_writer_pass_t : public writer_pass_t<console_writer_t> { public: void set_fd(int fd) { out.set_fd(fd); } };

class file_writer_t
{
  file_writer_t(const file_writer_t& other);
  file_writer_t& operator=(const file_writer_t& other);
#ifdef _WIN32
  HANDLE h;
#else
  int fd;
#endif

public:
#ifdef _WIN32
  file_writer_t() : h(INVALID_HANDLE_VALUE) {}
  ~file_writer_t() { if(h != INVALID_HANDLE_VALUE) CloseHandle(h); }
  void open(const char* path) { if(h != INVALID_HANDLE_VALUE) close(); h = CreateFile(path, GENERIC_WRITE, FILE_SHARE_READ, NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL); if(h == INVALID_HANDLE_VALUE) throw runtime_error("can't open output file"); }
  void write(const void* buf, size_t len, bool silent = 0) {
    DWORD num_written; BOOL success = WriteFile(h, buf, len, &num_written, NULL);
    if(!silent && (!success || num_written != len)) { throw runtime_error("unable to write"); }
  }
  void close() { if(h != INVALID_HANDLE_VALUE && !CloseHandle(h)) throw runtime_error("can't close output file"); h = INVALID_HANDLE_VALUE; }
#else
  int fd;
  file_writer_t() : fd(-1) {}
  ~file_writer_t() { if(fd >= 0) ::close(fd); }
  void open(const char* path) { if(fd < 0) close(); fd = ::open(path, O_WRONLY | O_TRUNC | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH); if(fd < 0) throw runtime_error("can't open output file"); }
  void write(const void* buf, size_t len, bool silent = 0) {
    for(char* begin = fd_buf; len;) {
      ssize_t num_written = ::write(fd, begin, len);
      if(num_written == (ssize_t)len) { break; }
      else if(num_written >= 0) { begin += num_written; len -= num_written; }
      else if(errno == EINTR) { continue; }
      else if(!silent) { throw runtime_error("unable to write"); }
    }
  }
  void close() { if(fd >= 0 && ::close(fd)) throw runtime_error("can't close output file"); fd = -1; }
#endif
};

class file_writer_pass_t : public writer_pass_t<file_writer_t> { public: void open(const char* path) { out.open(path); } void close() { out.close(); } };


////////////////////////////////////////////////////////////////////////////////////////////////
// tabular_writer
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t, typename output_base_t> class basic_tabular_writer_t : public input_base_t, public output_base_t
{
protected:
  int line;
  size_t column;
  vector<size_t> max_width;
  vector<char*> data;
  char* next;
  char* end;

  basic_tabular_writer_t() : line(0), column(0), next(0), end(0) {}
  void process_data();

public:
  void reinit(int more_passes = 0) { reinit_state(); }
  void reinit_state(int more_passes = 0);
  void process_token(const char* token, size_t len);
  void process_token(double token) { char buf[32]; size_t len = dtostr(token, buf); process_token(buf, len); }
  void process_line() { if(line == 19) process_data(); else if(!line || line > 19) this->output('\n'); ++line; column = 0; }
  void process_stream() { if(line <= 19) process_data(); this->flush(); }
};

class tabular_writer : public basic_tabular_writer_t<empty_class_t, console_writer_pass_t> { public: tabular_writer() {} tabular_writer(int fd) { set_fd(fd); } };
class dynamic_tabular_writer : public basic_tabular_writer_t<dynamic_pass_t, console_writer_pass_t> { public: dynamic_tabular_writer() {} dynamic_tabular_writer(int fd) { set_fd(fd); } };
class tabular_file_writer : public basic_tabular_writer_t<empty_class_t, file_writer_pass_t> { public: tabular_file_writer() {} tabular_file_writer(const char* path) { open(path); } };
class dynamic_tabular_file_writer : public basic_tabular_writer_t<dynamic_pass_t, file_writer_pass_t> { public: dynamic_tabular_file_writer() {} dynamic_tabular_file_writer(const char* path) { open(path); } };


////////////////////////////////////////////////////////////////////////////////////////////////
// csv_writer
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t, typename output_base_t> class basic_csv_writer_t : public input_base_t, public output_base_t
{
protected:
  int line;
  int num_columns;
  int column;

  basic_csv_writer_t() : line(0), num_columns(0), column(0) {}

public:
  void reinit(int more_passes = 0) { reinit_state(); }
  void reinit_state(int more_passes = 0) { line = 0; num_columns = 0; column = 0; }
  void process_token(const char* token, size_t len) { if(column) output_base_t::output(','); output_base_t::output(token, len); ++column; }
  void process_token(double token) { char buf[32]; size_t len = dtostr(token, buf); process_token(buf, len); }
  void process_line() {
    if(!line) { num_columns = column; }
    else if(column != num_columns) {
      stringstream msg; msg << "csv_writer: line " << line << " (zero's based) has " << column << " columns instead of " << num_columns;
      throw runtime_error(msg.str());
    }
    output_base_t::output('\n'); column = 0; ++line;
  }
  void process_stream() {
    this->flush();
    if(line && column) throw runtime_error("csv_writer saw process_stream called after process_token");
#ifdef TABLE_DIMENSIONS_DEBUG_PRINTS
    cerr << "csv_writer saw dimensions of " << num_columns << " by " << line << endl;
#endif
  }
};

class csv_writer : public basic_csv_writer_t<empty_class_t, console_writer_pass_t> { public: csv_writer() {} csv_writer(int fd) { set_fd(fd); } };
class dynamic_csv_writer : public basic_csv_writer_t<dynamic_pass_t, console_writer_pass_t> { public: dynamic_csv_writer() {} dynamic_csv_writer(int fd) { set_fd(fd); } };
class csv_file_writer : public basic_csv_writer_t<empty_class_t, file_writer_pass_t> { public: csv_file_writer() {} csv_file_writer(const char* path) { open(path); } };
class dynamic_csv_file_writer : public basic_csv_writer_t<dynamic_pass_t, file_writer_pass_t> { public: dynamic_csv_file_writer() {} dynamic_csv_file_writer(const char* path) { open(path); } };


////////////////////////////////////////////////////////////////////////////////////////////////
// driver
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename output_base_t> class csv_reader_base_t : public output_base_t
{
  csv_reader_base_t(const csv_reader_base_t<output_base_t>& other);
  csv_reader_base_t& operator=(const csv_reader_base_t<output_base_t>& other);

protected:
  int fd;
  size_t line;
  size_t num_keys;
  size_t column;
  bool return_on_eagain;
  char* buf;
  char* data_end;
  char* buf_end;

  csv_reader_base_t() : fd(-1), line(0), num_keys(0), column(0), return_on_eagain(0), buf(0) {}
  ~csv_reader_base_t() { delete[] buf; }
  void process(bool eof);

public:
  void reinit(int more_passes = 0) { reinit_state(); this->reinit_output_if(more_passes); }
  void reinit_state(int more_passes = 0) { line = 0; num_keys = 0; column = 0; return_on_eagain = 0; this->reinit_output_state_if(more_passes); }
  void set_return_on_eagain(bool val) { return_on_eagain = val; }
  int run();
};

template<typename output_base_t> class basic_csv_fd_reader_t : public csv_reader_base_t<output_base_t>
{
public:
  void set_fd(int fd) { this->fd = fd; }
};

template<typename out_t> class csv_reader : public basic_csv_fd_reader_t<single_output_pass_class_t<out_t> > {};

template<typename output_base_t> class basic_csv_file_reader_t : public csv_reader_base_t<output_base_t>
{
public:
  ~basic_csv_file_reader_t() { if(this->fd >= 0) ::close(this->fd); }
  void open(const char* path) { if(this->fd < 0) ::close(this->fd); this->fd = ::open(path, O_RDONLY); if(this->fd < 0) throw runtime_error("can't open output file"); }
  void close() { if(this->fd >= 0 && ::close(this->fd)) throw runtime_error("can't close output file"); }
};

template<typename out_t> class csv_file_reader : public basic_csv_file_reader_t<single_output_pass_class_t<out_t> > {};


////////////////////////////////////////////////////////////////////////////////////////////////
// threader
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t, typename output_base_t> class basic_threader_t : public input_base_t, public output_base_t
{
  basic_threader_t(const basic_threader_t<input_base_t, output_base_t>& other);
  basic_threader_t& operator=(const basic_threader_t<input_base_t, output_base_t>& other);

protected:
  struct chunk_info_t {
    char* start;
    char* end;

    chunk_info_t() : start(0) {}
  };
  static void* threader_main(void* data);

  chunk_info_t chunks[8];
  size_t write_chunk;
  char* write_chunk_next;
  size_t read_chunk;
  bool thread_created;
  pthread_mutex_t mutex;
  pthread_cond_t prod_cond;
  pthread_cond_t cons_cond;
  pthread_t thread;

  basic_threader_t() : thread_created(0) {}
  ~basic_threader_t();
  void resize_write_chunk(size_t min_size);
  void inc_write_chunk(bool term = 1);

public:
  void reinit(int more_passes = 0) { reinit_state(); this->reinit_output_if(more_passes); }
  void reinit_state(int more_passes = 0);
  void process_token(const char* token, size_t len);
  void process_token(double token);
  void process_line();
  void process_stream();
};

template<typename out_t> class threader : public basic_threader_t<empty_class_t, single_output_pass_class_t<out_t> > {};
template<typename out_t> class dynamic_threader : public basic_threader_t<dynamic_pass_t, single_output_pass_class_t<out_t> > {};
class dynamic_output_threader : public basic_threader_t<empty_class_t, single_output_pass_class_t<dynamic_pass_t*> > {};
class dynamic_output_dynamic_threader : public basic_threader_t<dynamic_pass_t, single_output_pass_class_t<dynamic_pass_t*> > {};


////////////////////////////////////////////////////////////////////////////////////////////////
// subset_tee
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t> class basic_subset_tee_t : public input_base_t
{
  basic_subset_tee_t(const basic_subset_tee_t& other);
  basic_subset_tee_t& operator=(const basic_subset_tee_t& other);

protected:
  struct dest_data_t
  {
    set<string> key;
    set<string> key_except;
    vector<pcre*> regex;
    vector<pcre*> regex_except;
    bool has_data;

    dest_data_t() : has_data(0) {}
    ~dest_data_t() {
      for(vector<pcre*>::iterator i = regex.begin(); i != regex.end(); ++i) pcre_free(*i);
      for(vector<pcre*>::iterator i = regex_except.begin(); i != regex_except.end(); ++i) pcre_free(*i);
    }
  };

  map<dynamic_pass_t*, dest_data_t> dest_data;
  typename map<dynamic_pass_t*, dest_data_t>::iterator ddi;
  bool first_row;
  int column;
  vector<pair<int, dynamic_pass_t*> > dest;
  vector<pair<int, dynamic_pass_t*> >::iterator di;

  basic_subset_tee_t() : ddi(dest_data.end()), first_row(1), column(0) {}

public:
  void reinit(int more_passes = 0) { dest_data.clear(); ddi = dest_data.end(); reinit_state(); if(more_passes == 0) return; if(more_passes > 0) --more_passes; for(typename map<dynamic_pass_t*, dest_data_t>::iterator ddi = dest_data.begin(); ddi != dest_data.end(); ++ddi) ddi->reinit(more_passes); }
  void reinit_state(int more_passes = 0) { first_row = 1; column = 0; this->dest.clear(); if(more_passes == 0) return; if(more_passes > 0) --more_passes; for(typename map<dynamic_pass_t*, dest_data_t>::iterator ddi = dest_data.begin(); ddi != dest_data.end(); ++ddi) ddi->reinit_state(more_passes); }
  void set_dest(dynamic_pass_t& dest) { ddi = dest_data.insert(map<dynamic_pass_t*, dest_data_t>::value_type(&dest, dest_data_t())).first; }
  void add_data(bool regex, const char* key);
  void add_exception(bool regex, const char* key);
  void process_token(const char* token, size_t len);
  void process_token(double token);
  void process_line();
  void process_stream() { for(typename map<dynamic_pass_t*, dest_data_t>::iterator ddi = dest_data.begin(); ddi != dest_data.end(); ++ddi) (*ddi).first->process_stream(); }
};

class subset_tee : public basic_subset_tee_t<empty_class_t> {};
class dynamic_subset_tee : public basic_subset_tee_t<dynamic_pass_t> {};


////////////////////////////////////////////////////////////////////////////////////////////////
// ordered_tee
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t> class basic_ordered_tee_t : public input_base_t {
  basic_ordered_tee_t(const basic_ordered_tee_t& other);
  basic_ordered_tee_t& operator=(const basic_ordered_tee_t& other);

protected:
  vector<dynamic_pass_t*> out;
  bool first_row;
  int num_columns;
  vector<char*> data;
  char* next;
  char* end;

  basic_ordered_tee_t() : first_row(1), num_columns(0), next(0), end(0) {}
  ~basic_ordered_tee_t() { for(vector<char*>::iterator i = data.begin(); i != data.end(); ++i) delete[] *i; }

public:
  void reinit(int more_passes = 0) { out.clear(); reinit_state(); if(more_passes == 0) return; if(more_passes > 0) --more_passes; for(typename vector<dynamic_pass_t*>::iterator i = out.begin(); i != out.end(); ++i) (*i)->reinit(more_passes); }
  void reinit_state(int more_passes = 0);
  void add_out(dynamic_pass_t& out) { this->out.push_back(&out); }
  void process_token(const char* token, size_t len);
  void process_token(double token);
  void process_line();
  void process_stream();
};

class ordered_tee : public basic_ordered_tee_t<empty_class_t> {};
class dynamic_ordered_tee : public basic_ordered_tee_t<dynamic_pass_t> {};


////////////////////////////////////////////////////////////////////////////////////////////////
// stacker
////////////////////////////////////////////////////////////////////////////////////////////////

enum stack_action_e
{
  ST_STACK,
  ST_LEAVE,
  ST_REMOVE
};

template<typename input_base_t, typename output_base_t> class basic_stacker_t : public input_base_t, public output_base_t
{
  basic_stacker_t(const basic_stacker_t<input_base_t, output_base_t>& other);
  basic_stacker_t& operator=(const basic_stacker_t<input_base_t, output_base_t>& other);

protected:
  struct regex_stack_action_t
  {
    pcre* regex;
    stack_action_e action;
  };

  stack_action_e default_action;
  map<string, stack_action_e> keyword_actions;
  vector<regex_stack_action_t> regex_actions;

  //header information
  bool first_line;
  vector<string> stack_keys;
  vector<stack_action_e> actions;
  size_t last_leave;

  //current line information
  size_t column;
  size_t stack_column;
  vector<pair<char*, char*> > leave_tokens;
  size_t leave_tokens_index;
  char* leave_tokens_next;
  vector<pair<char*, char*> > stack_tokens;
  size_t stack_tokens_index;
  char* stack_tokens_next;

  basic_stacker_t() : default_action(ST_REMOVE), first_line(1), last_leave(0), column(0), stack_column(0), leave_tokens_index(0), leave_tokens_next(0), stack_tokens_index(0), stack_tokens_next(0) {}
  ~basic_stacker_t();
  void resize(size_t len, vector<pair<char*, char*> >& tokens, size_t& index, char*& next);
  void process_leave_tokens();
  void process_stack_tokens();

public:
  void reinit(int more_passes = 0);
  void reinit_state(int more_passes = 0);
  void set_default_action(stack_action_e default_action) { this->default_action = default_action; }
  void add_action(bool regex, const char* key, stack_action_e action);
  void process_token(const char* token, size_t len);
  void process_token(double token);
  void process_line();
  void process_stream();
};

template<typename out_t> class stacker : public basic_stacker_t<empty_class_t, single_output_pass_class_t<out_t> > {};
template<typename out_t> class dynamic_stacker : public basic_stacker_t<dynamic_pass_t, single_output_pass_class_t<out_t> > {};
class dynamic_output_stacker : public basic_stacker_t<empty_class_t, single_output_pass_class_t<dynamic_pass_t*> > {};
class dynamic_output_dynamic_stacker : public basic_stacker_t<dynamic_pass_t, single_output_pass_class_t<dynamic_pass_t*> > {};


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

template<typename input_base_t, typename output_base_t> class basic_splitter_t : public input_base_t, public output_base_t
{
  basic_splitter_t(const basic_splitter_t<input_base_t, output_base_t>& other);
  basic_splitter_t& operator=(const basic_splitter_t<input_base_t, output_base_t>& other);

protected:
  struct regex_action_t { pcre* regex; split_action_e action; };

  split_action_e default_action;
  map<string, split_action_e> keyword_actions;
  vector<regex_action_t> regex_actions;

  bool first_line;
  vector<split_action_e> actions;
  vector<string> group_keys;
  vector<string> split_keys;

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

  map<char*, size_t, cstr_less> out_split_keys;
  vector<char*> group_storage;
  char* group_storage_next;
  char* group_storage_end;
  //typedef tr1::unordered_map<char*, vector<string>, multi_cstr_hash, multi_cstr_equal_to> data_t;
  typedef map<char*, vector<string>, multi_cstr_less> data_t;
  data_t data;

  basic_splitter_t() : default_action(SP_REMOVE), first_line(1), column(0),
                       group_tokens(new char[2048]), group_tokens_next(group_tokens), group_tokens_end(group_tokens + 2048),
                       split_by_tokens(new char[2048]), split_by_tokens_next(split_by_tokens), split_by_tokens_end(split_by_tokens + 2048),
                       split_tokens(new char[2048]), split_tokens_next(split_tokens), split_tokens_end(split_tokens + 2048),
                       group_storage_next(0), group_storage_end(0) {}
  ~basic_splitter_t();

public:
  void reinit(int more_passes = 0);
  void reinit_state(int more_passes = 0);
  void set_default_action(split_action_e default_action) { this->default_action = default_action; }
  void add_action(bool regex, const char* key, split_action_e action);
  void process_token(const char* token, size_t len);
  void process_line();
  void process_stream();
};

template<typename out_t> class splitter : public basic_splitter_t<empty_class_t, single_output_pass_class_t<out_t> > {};
template<typename out_t> class dynamic_splitter : public basic_splitter_t<dynamic_pass_t, single_output_pass_class_t<out_t> > {};
class dynamic_output_splitter : public basic_splitter_t<empty_class_t, single_output_pass_class_t<dynamic_pass_t*> > {};
class dynamic_output_dynamic_splitter : public basic_splitter_t<dynamic_pass_t, single_output_pass_class_t<dynamic_pass_t*> > {};


////////////////////////////////////////////////////////////////////////////////////////////////
// sorter
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t, typename output_base_t> class basic_sorter_t : public input_base_t, public output_base_t
{
  basic_sorter_t(const basic_sorter_t<input_base_t, output_base_t>& other);
  basic_sorter_t& operator=(const basic_sorter_t<input_base_t, output_base_t>& other);

protected:
  struct sorts_t {
    string key;
    uint8_t type;
  };
  struct row_t {
    char* sort;
    char* other;
    size_t other_index;
  };
  struct compare {
    const vector<sorts_t>& sorts;
    compare(const vector<sorts_t>& sorts) : sorts(sorts) {}
    bool operator() (const row_t& lhs, const row_t& rhs);
  };

  vector<sorts_t> sorts;

  vector<size_t> columns; //index into sorts or max for other
  vector<size_t>::const_iterator ci;
  size_t sorts_found;
  bool first_line;

  char** sort_buf;
  char** sort_buf_end;
  size_t sort_buf_len;
  vector<char*> sort_storage;
  char* sort_next;
  char* sort_end;
  vector<char*> other_storage;
  char* other_next;
  char* other_end;
  vector<row_t> rows;

  basic_sorter_t() : sorts_found(0), first_line(1), sort_buf(0), sort_buf_end(0), sort_next(0), sort_end(0), other_next(0), other_end(0) {}
  ~basic_sorter_t();

public:
  void reinit(int more_passes = 0) { reinit_state(); sorts.clear(); this->reinit_output_if(more_passes); }
  void reinit_state(int more_passes = 0);
  void add_sort(const char* key, bool ascending);
  void process_token(const char* token, size_t len);
  void process_line();
  void process_stream();
};

template<typename out_t> class sorter : public basic_sorter_t<empty_class_t, single_output_pass_class_t<out_t> > {};
template<typename out_t> class dynamic_sorter : public basic_sorter_t<dynamic_pass_t, single_output_pass_class_t<out_t> > {};
class dynamic_output_sorter : public basic_sorter_t<empty_class_t, single_output_pass_class_t<dynamic_pass_t*> > {};
class dynamic_output_dynamic_sorter : public basic_sorter_t<dynamic_pass_t, single_output_pass_class_t<dynamic_pass_t*> > {};


////////////////////////////////////////////////////////////////////////////////////////////////
// row_joiner
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t, typename output_base_t> class basic_row_joiner_t : public input_base_t, public output_base_t
{
  basic_row_joiner_t(const basic_row_joiner_t<input_base_t, output_base_t>& other);
  basic_row_joiner_t& operator=(const basic_row_joiner_t<input_base_t, output_base_t>& other);

protected:
  struct data_t {
    vector<char*> data;
    vector<char*>::iterator i;
    char* next;
    char* end;
  };

  vector<string> table_name;
  size_t table;

  bool first_line;
  bool more_lines;
  size_t column;
  vector<size_t> num_columns;
  vector<string> keys;

  vector<data_t> data;

  basic_row_joiner_t() {}
  ~basic_row_joiner_t();

public:
  void reinit(int more_passes = 0) { table_name.clear(); num_columns.clear(); reinit_state(); this->reinit_output_if(more_passes); }
  void reinit_state(int more_passes = 0);
  void add_table_name(const char* name = 0);
  void process_token(const char* token, size_t len);
  void process_token(double token);
  void process_line();
  void process_stream();
  void process_lines();
  void process();
};

template<typename out_t> class row_joiner : public basic_row_joiner_t<empty_class_t, single_output_pass_class_t<out_t> > {};
template<typename out_t> class dynamic_row_joiner : public basic_row_joiner_t<dynamic_pass_t, single_output_pass_class_t<out_t> > {};
class dynamic_output_row_joiner : public basic_row_joiner_t<empty_class_t, single_output_pass_class_t<dynamic_pass_t*> > {};
class dynamic_output_dynamic_row_joiner : public basic_row_joiner_t<dynamic_pass_t, single_output_pass_class_t<dynamic_pass_t*> > {};


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
  map<string, uint32_t> group_col;
  int end_stream;

  //incoming table info
  int cur_stream;
  int cur_stream_column;
  vector<column_info_t> column_info;
  const char* cur_group;
  const char* cur_group_cap;

  //outgoing table info
  map<string> keys;
  vector<col_match_joiner_group_storage_t> group_storage;
  size_t next_group_row;
  tr1::unordered_map<const char*, size_t> group_row;
  vector<vector<col_match_joiner_data_storage_t> > data_storage; //column then block of data

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
// col_pruner
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t, typename output_base_t> class basic_col_pruner_t : public input_base_t, public output_base_t
{
  basic_col_pruner_t(const basic_col_pruner_t<input_base_t, output_base_t>& other);
  basic_col_pruner_t& operator=(const basic_col_pruner_t<input_base_t, output_base_t>& other);

protected:
  bool first_row;
  bool passthrough;
  size_t column;
  size_t num_columns;
  size_t columns_with_data;
  uint32_t* has_data;
  vector<char*> data;
  char* next;
  char* end;

  basic_col_pruner_t() : has_data(0) {}
  ~basic_col_pruner_t();

public:
  void reinit(int more_passes = 0) { reinit_state(); this->reinit_output_if(more_passes); }
  void reinit_state(int more_passes = 0);
  void process_token(const char* token, size_t len);
  void process_token(double token);
  void process_line();
  void process_stream();
};

template<typename out_t> class col_pruner : public basic_col_pruner_t<empty_class_t, single_output_pass_class_t<out_t> > {};
template<typename out_t> class dynamic_col_pruner : public basic_col_pruner_t<dynamic_pass_t, single_output_pass_class_t<out_t> > {};
class dynamic_output_col_pruner : public basic_col_pruner_t<empty_class_t, single_output_pass_class_t<dynamic_pass_t*> > {};
class dynamic_output_dynamic_col_pruner : public basic_col_pruner_t<dynamic_pass_t, single_output_pass_class_t<dynamic_pass_t*> > {};


////////////////////////////////////////////////////////////////////////////////////////////////
// combiner
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t, typename output_base_t> class basic_combiner_t : public input_base_t, public output_base_t
{
  basic_combiner_t(const basic_combiner_t<input_base_t, output_base_t>& other);
  basic_combiner_t& operator=(const basic_combiner_t<input_base_t, output_base_t>& other);

protected:
  vector<pair<pcre*, string> > pairs;
  bool first_row;
  int column;
  vector<int> remap_indexes;
  vector<string> tokens;

  basic_combiner_t() : first_row(1), column(0) {}
  ~basic_combiner_t() { for(vector<pair<pcre*, string> >::iterator i = pairs.begin(); i != pairs.end(); ++i) pcre_free((*i).first); }

public:
  void reinit(int more_passes = 0) { for(vector<pair<pcre*, string> >::iterator i = pairs.begin(); i != pairs.end(); ++i) { pcre_free((*i).first); } pairs.clear(); reinit_state(); this->reinit_output_if(more_passes); }
  void reinit_state(int more_passes = 0) { first_row = 1; column = 0; remap_indexes.clear(); tokens.clear(); this->reinit_output_state_if(more_passes); }
  void add_pair(const char* from, const char* to);
  void process_token(const char* token, size_t len);
  void process_line();
  void process_stream() { this->output_stream(); }
};

template<typename out_t> class combiner : public basic_combiner_t<empty_class_t, single_output_pass_class_t<out_t> > {};
template<typename out_t> class dynamic_combiner : public basic_combiner_t<dynamic_pass_t, single_output_pass_class_t<out_t> > {};
class dynamic_output_combiner : public basic_combiner_t<empty_class_t, single_output_pass_class_t<dynamic_pass_t*> > {};
class dynamic_output_dynamic_combiner : public basic_combiner_t<dynamic_pass_t, single_output_pass_class_t<dynamic_pass_t*> > {};


////////////////////////////////////////////////////////////////////////////////////////////////
// summarizer
////////////////////////////////////////////////////////////////////////////////////////////////

enum summarizer_flags {
  SUM_MISSING =  0x0004,
  SUM_COUNT =    0x0008,
  SUM_SUM =      0x0010,
  SUM_MIN =      0x0020,
  SUM_MAX =      0x0040,
  SUM_AVG =      0x0080,
  SUM_VARIANCE = 0x0100,
  SUM_STD_DEV =  0x0200
};

template<typename input_base_t, typename output_base_t> class basic_summarizer_t : public input_base_t, public output_base_t {
  basic_summarizer_t(const basic_summarizer_t<input_base_t, output_base_t>& other);
  basic_summarizer_t& operator=(const basic_summarizer_t<input_base_t, output_base_t>& other);

protected:
  struct data_t {
    int missing;
    int count;
    double sum;
    double sum_of_squares;
    double min;
    double max;

    data_t() : missing(0), count(0), sum(0.0), sum_of_squares(0.0), min(numeric_limits<double>::infinity()), max(-numeric_limits<double>::infinity()) {}
  };

  vector<pcre*> pre_sorted_group_regexes;
  vector<pcre*> group_regexes;
  vector<pair<pcre*, uint32_t> > data_regexes;
  vector<pcre*> exception_regexes;

  bool first_line;
  vector<uint32_t> column_flags;
  size_t num_data_columns;

  vector<uint32_t>::const_iterator cfi;
  double* values;
  double* vi;
  char* pre_sorted_group_tokens;
  char* pre_sorted_group_tokens_next;
  char* pre_sorted_group_tokens_end;
  char* group_tokens;
  char* group_tokens_next;
  char* group_tokens_end;

  char* pre_sorted_group_storage;
  char* pre_sorted_group_storage_end;
  vector<char*> group_storage;
  char* group_storage_next;
  char* group_storage_end;
  vector<data_t*> data_storage;
  data_t* data_storage_next;
  data_t* data_storage_end;
  //typedef tr1::unordered_map<char*, data_t*, multi_cstr_hash, multi_cstr_equal_to> data_t;
  typedef map<char*, data_t*, multi_cstr_less> data_map_t;
  data_map_t data;

  basic_summarizer_t() : values(0), pre_sorted_group_tokens(0), group_tokens(0), pre_sorted_group_storage(0) { reinit(); }
  ~basic_summarizer_t();
  void print_header(char*& buf, char*& next, char*& end, const char* op, size_t op_len, const char* token, size_t len);
  void print_data();

public:
  void reinit(int more_passes = 0);
  void reinit_state(int more_passes = 0);
  void add_group(const char* regex, bool pre_sorted = 0);
  void add_data(const char* regex, uint32_t flags);
  void add_exception(const char* regex);
  void process_token(const char* token, size_t len);
  void process_token(double token);
  void process_line();
  void process_stream() { print_data(); output_base_t::output_stream(); }
};

template<typename out_t> class summarizer : public basic_summarizer_t<empty_class_t, single_output_pass_class_t<out_t> > {};
template<typename out_t> class dynamic_summarizer : public basic_summarizer_t<dynamic_pass_t, single_output_pass_class_t<out_t> > {};
class dynamic_output_summarizer : public basic_summarizer_t<empty_class_t, single_output_pass_class_t<dynamic_pass_t*> > {};
class dynamic_output_dynamic_summarizer : public basic_summarizer_t<dynamic_pass_t, single_output_pass_class_t<dynamic_pass_t*> > {};


////////////////////////////////////////////////////////////////////////////////////////////////
// range_stacker
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t, typename output_base_t> class basic_range_stacker_t : public input_base_t, public output_base_t
{
  basic_range_stacker_t(const basic_range_stacker_t<input_base_t, output_base_t>& other);
  basic_range_stacker_t& operator=(const basic_range_stacker_t<input_base_t, output_base_t>& other);

protected:
  struct range_t {
    string start_name;
    size_t start_col_index;
    string stop_name;
    size_t stop_col_index;
    string new_col_name;
    double cur_val;
  };

  struct col_t { size_t col; double val; };

  vector<range_t> ranges;
  bool first_row;
  size_t column;
  vector<col_t> columns;
  typename vector<col_t>::iterator ci;
  vector<pair<char*, char*> > leave_tokens;
  size_t leave_tokens_index;
  char* leave_tokens_next;

  basic_range_stacker_t() : first_row(1), column(0), leave_tokens_index(0), leave_tokens_next(0) {}
  ~basic_range_stacker_t() { for(vector<pair<char*, char*> >::iterator i = leave_tokens.begin(); i != leave_tokens.end(); ++i) delete[] (*i).first; }

public:
  void reinit(int more_passes = 0) { ranges.clear(); reinit_state(); this->reinit_output_if(more_passes); }
  void reinit_state(int more_passes = 0);
  void add(const char* start, const char* stop, const char* new_name);
  void process_token(const char* token, size_t len);
  void process_token(double token);
  void process_line();
  void process_stream();
};

template<typename out_t> class range_stacker : public basic_range_stacker_t<empty_class_t, single_output_pass_class_t<out_t> > {};
template<typename out_t> class dynamic_range_stacker : public basic_range_stacker_t<dynamic_pass_t, single_output_pass_class_t<out_t> > {};
class dynamic_output_range_stacker : public basic_range_stacker_t<empty_class_t, single_output_pass_class_t<dynamic_pass_t*> > {};
class dynamic_output_dynamic_range_stacker : public basic_range_stacker_t<dynamic_pass_t, single_output_pass_class_t<dynamic_pass_t*> > {};


////////////////////////////////////////////////////////////////////////////////////////////////
// base_converter
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t, typename output_base_t> class basic_base_converter_t : public input_base_t, public output_base_t
{
  basic_base_converter_t(const basic_base_converter_t<input_base_t, output_base_t>& other);
  basic_base_converter_t& operator=(const basic_base_converter_t<input_base_t, output_base_t>& other);

protected:
  struct regex_base_conv_t {
    pcre* regex;
    int from;
    int to;

    regex_base_conv_t() : regex(0) {}
    ~regex_base_conv_t() { pcre_free(regex); }
  };

  struct conv_t { int from; int to; };

  vector<regex_base_conv_t> regex_base_conv;
  bool first_row;
  int column;
  vector<conv_t> conv;

  basic_base_converter_t() : first_row(1), column(0) {}
  ~basic_base_converter_t() {}

public:
  void reinit(int more_passes = 0) { reinit_state(); this->reinit_output_if(more_passes); }
  void reinit_state(int more_passes = 0) { regex_base_conv.clear(); first_row = 1; column = 0; conv.clear(); this->reinit_output_state_if(more_passes); }
  void add_conv(const char* regex, int from, int to);
  void process_token(const char* token, size_t len);
  void process_token(double token);
  void process_line() { if(first_row) { first_row = 0; } column = 0; this->output_line(); }
  void process_stream() { this->output_stream(); }
};

template<typename out_t> class base_converter : public basic_base_converter_t<empty_class_t, single_output_pass_class_t<out_t> > {};
template<typename out_t> class dynamic_base_converter : public basic_base_converter_t<dynamic_pass_t, single_output_pass_class_t<out_t> > {};
class dynamic_output_base_converter : public basic_base_converter_t<empty_class_t, single_output_pass_class_t<dynamic_pass_t*> > {};
class dynamic_output_dynamic_base_converter : public basic_base_converter_t<dynamic_pass_t, single_output_pass_class_t<dynamic_pass_t*> > {};


////////////////////////////////////////////////////////////////////////////////////////////////
// variance_analyzer
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t, typename output_base_t> class basic_variance_analyzer_t : public input_base_t, public output_base_t
{
  basic_variance_analyzer_t(const basic_variance_analyzer_t<input_base_t, output_base_t>& other);
  basic_variance_analyzer_t& operator=(const basic_variance_analyzer_t<input_base_t, output_base_t>& other);

protected:
  struct treatment_data_t {
    int count;
    double sum;
    double sum_of_squares;

    treatment_data_t() : count(0), sum(0.0), sum_of_squares(0.0) {}
  };

  vector<pcre*> group_regexes;
  vector<pcre*> data_regexes;
  vector<pcre*> exception_regexes;

  bool first_line;
  vector<char> column_type; // 0 = ignore, 1 = group, 2 = data
  vector<string> data_keywords;

  vector<char>::const_iterator cti;
  char* group_tokens;
  char* group_tokens_next;
  char* group_tokens_end;
  vector<char*> group_storage;
  char* group_storage_next;
  char* group_storage_end;
  typedef map<char*, size_t, multi_cstr_less> groups_t;
  groups_t groups;
  double* values;
  double* vi;
  typedef vector<treatment_data_t*> data_t;
  data_t data; // keyword fast, group/treatment slow

  basic_variance_analyzer_t() : group_tokens(0), values(0) { reinit(); }
  ~basic_variance_analyzer_t();

public:
  void reinit(int more_passes = 0);
  void reinit_state(int more_passes = 0);
  void add_group(const char* regex);
  void add_data(const char* regex);
  void add_exception(const char* regex);
  void process_token(const char* token, size_t len);
  void process_token(double token);
  void process_line();
  void process_stream();
};

template<typename out_t> class variance_analyzer : public basic_variance_analyzer_t<empty_class_t, single_output_pass_class_t<out_t> > {};
template<typename out_t> class dynamic_variance_analyzer : public basic_variance_analyzer_t<dynamic_pass_t, single_output_pass_class_t<out_t> > {};
class dynamic_output_variance_analyzer : public basic_variance_analyzer_t<empty_class_t, single_output_pass_class_t<dynamic_pass_t*> > {};
class dynamic_output_dynamic_variance_analyzer : public basic_variance_analyzer_t<dynamic_pass_t, single_output_pass_class_t<dynamic_pass_t*> > {};


////////////////////////////////////////////////////////////////////////////////////////////////
// templates to allow regular functions
////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////
// substituter
////////////////////////////////////////////////////////////////////////////////////////////////

class substituter {
  struct data_t {
    char* buf;
    char* end;
    pcre* from;
    pcre_extra* from_extra;
    string to;
    int rc;
  } *data;

public:
  substituter() : data(0) {}
  substituter(const char* from, const char* to) : data(0) { init(from, to); }
  substituter(const substituter& other) { data = other.data; if(data) ++data->rc; }
  ~substituter() {
    if(!data) return;
    if(!--data->rc) { pcre_free_study(data->from_extra); pcre_free(data->from); delete[] data->buf; delete data; }
  }
  void operator=(const substituter& other) {
    if(data && !--data->rc) { pcre_free_study(data->from_extra); pcre_free(data->from); delete[] data->buf; delete data; }
    data = other.data;
    if(data) ++data->rc;
  }
  void init(const char* from, const char* to) {
    if(data) {
      if(data->rc <= 1) { pcre_free_study(data->from_extra); pcre_free(data->from); }
      else { --data->rc; data = new data_t; data->buf = new char[2048]; data->end = data->buf + 2048; data->rc = 1; }
    }
    else { data = new data_t; data->buf = new char[2048]; data->end = data->buf + 2048; data->rc = 1; }

    const char* err; int err_off;
    data->from = pcre_compile(from, 0, &err, &err_off, 0); if(!data->from) throw runtime_error("substitutor can't compile from regex");
    data->from_extra = pcre_study(data->from, PCRE_STUDY_JIT_COMPILE, &err); if(!data->from_extra) throw runtime_error("substitutor can't study from");
    data->to = to;
  }
  c_str_and_len_t operator()(c_str_and_len_t in) {
    c_str_and_len_t ret(in);
    int ovector[30]; int rc = pcre_exec(data->from, data->from_extra, in.c_str, in.len, 0, 0, ovector, 30);
    if(rc < 0) { if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("substitutor exception match error"); }
    else {
      char* next = data->buf; if(!rc) rc = 10;
      generate_substitution(in.c_str, data->to.c_str(), ovector, rc, data->buf, next, data->end);
      ret.c_str = data->buf; ret.len = next - data->buf - 1;
    }
    return ret;
  }
};


////////////////////////////////////////////////////////////////////////////////////////////////
// unary_col_adder
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t, typename output_base_t, typename op_in_t, typename op_out_t, typename op_t>
class basic_unary_col_adder_t : public input_base_t, public output_base_t
{
  basic_unary_col_adder_t(const basic_unary_col_adder_t<input_base_t, output_base_t, op_in_t, op_out_t, op_t>& other);
  basic_unary_col_adder_t& operator=(const basic_unary_col_adder_t<input_base_t, output_base_t, op_in_t, op_out_t, op_t>& other);

protected:
  struct inst_t {
    pcre* regex;
    bool remove_source;
    string new_key;
    op_t op;
  };

  struct col_t { size_t col; bool passthrough; vector<op_t> ops; };

  vector<inst_t> insts;
  bool first_row;
  size_t column;
  char* buf;
  char* end;
  vector<col_t> columns;
  typename vector<col_t>::iterator ci;

  basic_unary_col_adder_t() : first_row(1), column(0), buf(new char[2048]), end(buf + 2048) {}
  ~basic_unary_col_adder_t() { for(typename vector<inst_t>::iterator i = insts.begin(); i != insts.end(); ++i) pcre_free((*i).regex); delete[] buf; }
  c_str_and_len_t get_in_value(const char* token, size_t len, c_str_and_len_t* dummy) { return c_str_and_len_t(token, len); }
  c_str_and_len_t get_in_value(double token, char* buf, c_str_and_len_t* dummy) { c_str_and_len_t ret; ret.c_str = buf; ret.len = dtostr(token, buf); return ret; }
  double get_in_value(const char* token, size_t len, double* dummy) { char* next; double ret = strtod(token, &next); if(next == token) ret = numeric_limits<double>::quiet_NaN(); return ret; }
  double get_in_value(double token, char* buf, double* dummy) { return token; }
  using output_base_t::output_token;
  void output_token(c_str_and_len_t& val) { output_base_t::output_token(val.c_str, val.len); }

public:
  void reinit(int more_passes = 0) { for(typename vector<inst_t>::iterator i = insts.begin(); i != insts.end(); ++i) pcre_free((*i).regex); insts.clear(); reinit_state(); this->reinit_output_if(more_passes); }
  void reinit_state(int more_passes = 0) { first_row = 1; column = 0; delete[] buf; buf = new char[2048]; end = buf + 2048; columns.clear(); this->reinit_output_state_if(more_passes); }
  void add(const char* regex, const char* new_key, const op_t& op, bool remove_source = 0);
  void process_token(const char* token, size_t len);
  void process_token(double token);
  void process_line() { if(first_row) { first_row = 0; } this->output_line(); column = 0; ci = columns.begin(); }
  void process_stream() { this->output_stream(); }
};

template<typename out_t> class unary_col_adder : public basic_unary_col_adder_t<empty_class_t, single_output_pass_class_t<out_t>, double, double, double (*)(double)> {};
template<typename out_t> class unary_c_str_col_adder : public basic_unary_col_adder_t<empty_class_t, single_output_pass_class_t<out_t>, c_str_and_len_t, c_str_and_len_t, c_str_and_len_t (*)(c_str_and_len_t)> {};
template<typename out_t> class unary_double_c_str_col_adder : public basic_unary_col_adder_t<empty_class_t, single_output_pass_class_t<out_t>, double, c_str_and_len_t, c_str_and_len_t (*)(double)> {};
template<typename out_t> class unary_c_str_double_col_adder : public basic_unary_col_adder_t<empty_class_t, single_output_pass_class_t<out_t>, c_str_and_len_t, double, double (*)(c_str_and_len_t)> {};
template<typename out_t> class substitute_col_adder : public basic_unary_col_adder_t<empty_class_t, single_output_pass_class_t<out_t>, c_str_and_len_t, c_str_and_len_t, substituter> {};


////////////////////////////////////////////////////////////////////////////////////////////////
// binary_col_adder
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t, typename output_base_t, typename op_in1_t, typename op_in2_t, typename op_out_t, typename op_t>
class basic_binary_col_adder_t : public input_base_t, public output_base_t
{
  basic_binary_col_adder_t(const basic_binary_col_adder_t& other);
  basic_binary_col_adder_t& operator=(const basic_binary_col_adder_t& other);

protected:
  struct inst_t {
    pcre* regex;
    bool remove_source;
    string other_key;
    bool remove_other;
    string new_key;
    op_t op;
  };

  struct col_info_t { size_t index; bool need_double; bool need_c_str; bool passthrough; };
  struct new_col_info_t { size_t other_col; string new_key; op_t* op; };
  struct col_t { size_t col; bool need_double; double double_val; bool need_c_str; c_str_and_len_t c_str_val; const char* c_str_end; bool passthrough; };
  struct new_col_t { size_t col_index; size_t other_col_index; op_t op; };

  vector<inst_t> insts;
  bool first_row;
  size_t column;
  vector<char*> key_storage;
  char* key_storage_next;
  char* key_storage_end;
  vector<char*> keys;
  vector<col_t> columns;
  typename vector<col_t>::iterator ci;
  vector<new_col_t> new_columns;

  basic_binary_col_adder_t() { reinit(); }
  ~basic_binary_col_adder_t();
  c_str_and_len_t& get_in_value(size_t index, c_str_and_len_t* dummy) { return columns[index].c_str_val; }
  double& get_in_value(size_t index, double* dummy) { return columns[index].double_val; }
  using output_base_t::output_token;
  void output_token(c_str_and_len_t& val) { output_token(val.c_str, val.len); }

public:
  void reinit(int more_passes = 0) { for(typename vector<inst_t>::iterator i = insts.begin(); i != insts.end(); ++i) pcre_free((*i).regex); insts.clear(); this->reinit_state(); }
  void reinit_state(int more_passes = 0);
  void add(const char* regex, const char* other_key, const char* new_key, const op_t& op, bool remove_source = 0, bool remove_other = 0);
  void process_token(const char* token, size_t len);
  void process_token(double token);
  void process_line();
  void process_stream() { for(ci = columns.begin(); ci != columns.end(); ++ci) { delete[] (*ci).c_str_val.c_str; } columns.clear(); this->output_stream(); }
};

template<typename out_t> class binary_col_adder : public basic_binary_col_adder_t<empty_class_t, single_output_pass_class_t<out_t>, double, double, double, double (*)(double, double)> {};
template<typename out_t> class binary_c_str_col_adder : public basic_binary_col_adder_t<empty_class_t, single_output_pass_class_t<out_t>, c_str_and_len_t, c_str_and_len_t, c_str_and_len_t, c_str_and_len_t (*)(c_str_and_len_t, c_str_and_len_t)> {};
template<typename out_t> class binary_double_c_str_col_adder : public basic_binary_col_adder_t<empty_class_t, single_output_pass_class_t<out_t>, double, double, c_str_and_len_t, c_str_and_len_t (*)(double, double)> {};
template<typename out_t> class binary_c_str_double_col_adder : public basic_binary_col_adder_t<empty_class_t, single_output_pass_class_t<out_t>, c_str_and_len_t, c_str_and_len_t, double, double (*)(c_str_and_len_t, c_str_and_len_t)> {};


}


////////////////////////////////////////////////////////////////////////////////////////////////
// implementation stuff that can change with TABLE_MINOR
////////////////////////////////////////////////////////////////////////////////////////////////

#include <typeinfo>
#include <math.h>
#include <stdio.h>


namespace table {

using namespace std;

////////////////////////////////////////////////////////////////////////////////////////////////
// tabular_writer
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t, typename output_base_t> void basic_tabular_writer_t<input_base_t, output_base_t>::process_data()
{
  if(!next) return;

  *next++ = '\03';

  size_t c = 0;
  char buf[32];
  for(; c < max_width.size(); ++c) {
    int len = sprintf(buf, "c%zu", c);
    size_t spaces = 1 + max_width[c] - len;
    for(size_t i = 0; i < spaces; ++i) this->output(' '); 
    this->output(buf, len);
  }
  this->output('\n');

  c = 0;
  for(vector<char*>::iterator i = data.begin(); i != data.end(); ++i) {
    next = *i;
    while(*next != '\03') {
      size_t len = strlen(next);
      size_t spaces = 1 + max_width[c] - len;
      for(size_t i = 0; i < spaces; ++i) this->output(' ');
      this->output(next, len);
      next += len + 1;
      if(++c >= max_width.size()) { this->output('\n'); c = 0; }
    }
    delete[] *i;
  }
  max_width.clear();
  data.clear();
}

template<typename input_base_t, typename output_base_t> void basic_tabular_writer_t<input_base_t, output_base_t>::reinit_state(int more_passes)
{
  line = 0;
  column = 0;
  max_width.clear();
  for(vector<char*>::iterator i = data.begin(); i != data.end(); ++i) delete[] *i;
  data.clear();
  next = 0;
  end = 0;
}

template<typename input_base_t, typename output_base_t> void basic_tabular_writer_t<input_base_t, output_base_t>::process_token(const char* token, size_t len)
{
  if(!line) {
    char lbuf[32]; int clen = sprintf(lbuf, "%zu", column);
    max_width.push_back(clen + 1);
    this->output('c');
    this->output(lbuf, clen);
    this->output('=');
    this->output(token, len);
    this->output('\n');
  }
  else if(line < 20) {
    if(max_width[column] < len) max_width[column] = len;
    if(!next || next + len + 2 > end) {
      if(next) *next++ = '\03';
      size_t cap = 32 * 1024;
      if(cap < len + 2) cap = len + 2;
      data.push_back(new char[cap]);
      next = data.back();
      end = data.back() + cap;
    }
    memcpy(next, token, len); next += len;
    *next++ = '\0';
  }
  else {
    if(max_width[column] < len) max_width[column] = len;
    size_t spaces = 1 + max_width[column] - len;
    for(size_t i = 0; i < spaces; ++i) this->output(' ');
    this->output(token, len);
  }

  ++column;
}


////////////////////////////////////////////////////////////////////////////////////////////////
// driver
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename output_base_t> void csv_reader_base_t<output_base_t>::process(bool eof)
{
  for(const char* start = buf; 1;) { // tokens
    const char* end = start;
    while(end < data_end && *end != '\n' && *end != ',') ++end;

    if(!eof && end == data_end) {
      if(start != buf) { const size_t len = end - start; memcpy(buf, start, len); data_end = buf + len; }
      break;
    }

    if(end == data_end) { if(column || start != end) this->output_token(start, end - start); }
    else if(*end == ',') { this->output_token(start, end - start); ++column; }
    else {
      this->output_token(start, end - start);
      if(!line) { num_keys = column; }
      else if(column != num_keys && (column || start != end)) {
        stringstream msg; msg << "Line " << line << " had " << column << " columns when the first line had " << num_keys;
        throw runtime_error(msg.str());
      }
      this->output_line();
      ++line;
      column = 0;
    }

    if(end == data_end) { data_end = buf; break; }
    else start = end + 1;
  }
}

template<typename output_base_t> int csv_reader_base_t<output_base_t>::run()
{
  if(fd < 0) throw runtime_error("no fd");

  const size_t read_size = 32 * 1024;

  if(!buf) {
    buf = new char[read_size + 128];
    data_end = buf;
    buf_end = buf + read_size + 128;
  }

  ssize_t num_read; do {
    if(data_end + read_size + 1 > buf_end) resize_buffer(buf, data_end, buf_end, read_size);
    num_read = ::read(fd, data_end, read_size);
    if(num_read < 0) {
      if(errno != EAGAIN) throw runtime_error("couldn't read");
      if(return_on_eagain) { return 1; }
      else continue;
    }
    else data_end += num_read;

    process(num_read == 0);
  } while(num_read > 0);

#ifdef TABLE_DIMENSIONS_DEBUG_PRINTS
  cerr << "read_csv saw dimensions of " << num_keys << " by " << line << endl;
#endif
  this->output_stream();

  return num_read;
}

////////////////////////////////////////////////////////////////////////////////////////////////
// threader
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t, typename output_base_t> void* basic_threader_t<input_base_t, output_base_t>::threader_main(void* data)
{
  basic_threader_t<input_base_t, output_base_t>& t = *static_cast<basic_threader_t<input_base_t, output_base_t>*>(data);

  bool done = 0;
  bool inc = 0;
  while(!done) {
    pthread_mutex_lock(&t.mutex);
    if(inc) {
      t.read_chunk = (t.read_chunk + 1) % 8;
      pthread_cond_signal(&t.prod_cond);
    }
    else inc = 1;
    while(t.read_chunk == t.write_chunk) { pthread_cond_wait(&t.cons_cond, &t.mutex); }
    pthread_mutex_unlock(&t.mutex);

    for(char* cur = t.chunks[t.read_chunk].start; 1; ++cur) {
      if(*cur == '\x01') { t.output_token(*reinterpret_cast<double*>(++cur)); cur += sizeof(double) - 1; }
      else if(*cur == '\x02') { t.output_line(); }
      else if(*cur == '\x03') { break; }
      else if(*cur == '\x04') { done = 1; break; }
      else { size_t len = strlen(cur); t.output_token(cur, len); cur += len; }
    }
  }

  t.output_stream();

  return 0;
}

template<typename input_base_t, typename output_base_t> basic_threader_t<input_base_t, output_base_t>::~basic_threader_t()
{
  if(thread_created) {
    pthread_cancel(thread);
    pthread_cond_destroy(&cons_cond);
    pthread_cond_destroy(&prod_cond);
    pthread_mutex_destroy(&mutex);
    thread_created = 0;
  }
  for(int c = 0; c < 8; ++c) delete[] chunks[c].start;
}

template<typename input_base_t, typename output_base_t> void basic_threader_t<input_base_t, output_base_t>::resize_write_chunk(size_t min_size)
{
  delete[] chunks[write_chunk].start;
  chunks[write_chunk].start = new char[min_size * 2];
  write_chunk_next = chunks[write_chunk].start;
  chunks[write_chunk].end = chunks[write_chunk].start + min_size * 2;
}

template<typename input_base_t, typename output_base_t> void basic_threader_t<input_base_t, output_base_t>::inc_write_chunk(bool term)
{
    if(term) *write_chunk_next++ = '\x03';

    pthread_mutex_lock(&mutex);
    while((write_chunk + 1) % 8 == read_chunk)
      pthread_cond_wait(&prod_cond, &mutex);
    write_chunk = (write_chunk + 1) % 8;
    pthread_cond_signal(&cons_cond);
    pthread_mutex_unlock(&mutex);

    write_chunk_next = chunks[write_chunk].start;
}

template<typename input_base_t, typename output_base_t> void basic_threader_t<input_base_t, output_base_t>::reinit_state(int more_passes) {
  for(int c = 0; c < 8; ++c) {
    delete[] chunks[c].start;
    chunks[c].start = new char[8 * 1024];
    chunks[c].end = chunks[c].start + 8 * 1024;
  }
  write_chunk = 0;
  write_chunk_next = chunks[0].start;
  read_chunk = 0;
  this->reinit_output_state_if(more_passes);
}

template<typename input_base_t, typename output_base_t> void basic_threader_t<input_base_t, output_base_t>::process_token(const char* token, size_t len)
{
  if(!thread_created) {
    pthread_mutex_init(&mutex, 0);
    pthread_cond_init(&prod_cond, 0);
    pthread_cond_init(&cons_cond, 0);
    pthread_create(&thread, 0, basic_threader_t<input_base_t, output_base_t>::threader_main, this);
    thread_created = 1;
  }

  if(size_t(chunks[write_chunk].end - write_chunk_next) < len + 2) {
    if(write_chunk_next == chunks[write_chunk].start) resize_write_chunk(len + 2);
    else inc_write_chunk();
  }
  memcpy(write_chunk_next, token, len); write_chunk_next += len;
  *write_chunk_next++ = '\0';
}

template<typename input_base_t, typename output_base_t> void basic_threader_t<input_base_t, output_base_t>::process_token(double token)
{
  if(!thread_created) {
    pthread_mutex_init(&mutex, 0);
    pthread_cond_init(&prod_cond, 0);
    pthread_cond_init(&cons_cond, 0);
    pthread_create(&thread, 0, basic_threader_t<input_base_t, output_base_t>::threader_main, this);
    thread_created = 1;
  }

  if(size_t(chunks[write_chunk].end - write_chunk_next) < size_t(sizeof(double) + 2)) inc_write_chunk();
  *write_chunk_next++ = '\x01';
  memcpy(write_chunk_next, &token, sizeof(double));
  write_chunk_next += sizeof(double);
}

template<typename input_base_t, typename output_base_t> void basic_threader_t<input_base_t, output_base_t>::process_line()
{
  if(!thread_created) { this->output_line(); return; }

  if(chunks[write_chunk].end - write_chunk_next < 2) inc_write_chunk();
  *write_chunk_next++ = '\x02';
}

template<typename input_base_t, typename output_base_t> void basic_threader_t<input_base_t, output_base_t>::process_stream()
{
  if(!thread_created) { this->output_stream(); return; }

  *write_chunk_next++ = '\x04';
  inc_write_chunk(0);

  pthread_join(thread, 0);
  pthread_cond_destroy(&cons_cond);
  pthread_cond_destroy(&prod_cond);
  pthread_mutex_destroy(&mutex);
  thread_created = 0;
}


////////////////////////////////////////////////////////////////////////////////////////////////
// subset_tee
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t> void basic_subset_tee_t<input_base_t>::add_data(bool regex, const char* key)
{
  if(ddi == dest_data.end()) throw runtime_error("no current destination");

  if(regex) {
    const char* err; int err_off; pcre* p = pcre_compile(key, 0, &err, &err_off, 0);
    if(!p) throw runtime_error("subset_tee can't compile data regex");
    (*ddi).second.regex.push_back(p);
  }
  else (*ddi).second.key.insert(key);
}

template<typename input_base_t> void basic_subset_tee_t<input_base_t>::add_exception(bool regex, const char* key)
{
  if(ddi == dest_data.end()) throw runtime_error("no current destination");

  if(regex) {
    const char* err; int err_off; pcre* p = pcre_compile(key, 0, &err, &err_off, 0);
    if(!p) throw runtime_error("subset_tee can't compile exception regex");
    (*ddi).second.regex_except.push_back(p);
  }
  else (*ddi).second.key_except.insert(key);
}

template<typename input_base_t> void basic_subset_tee_t<input_base_t>::process_token(const char* token, size_t len)
{
  if(first_row) {
    for(typename map<dynamic_pass_t*, dest_data_t>::iterator di = dest_data.begin(); di != dest_data.end(); ++di) {
      dest_data_t& d = (*di).second;

      bool add = 0;
      string stoken(token, len);
      set<string>::const_iterator ki = d.key.find(stoken);
      if(ki != d.key.end()) add = 1;
      else {
        for(vector<pcre*>::const_iterator ri = d.regex.begin(); ri != d.regex.end(); ++ri) {
          int ovector[30]; int rc = pcre_exec(*ri, 0, token, len, 0, 0, ovector, 30);
          if(rc >= 0) { add = 1; break; }
          else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("subset_tee match error");
        }
      }

      set<string>::const_iterator kei = d.key_except.find(stoken);
      if(kei != d.key_except.end()) add = 0;
      else {
        for(vector<pcre*>::const_iterator ri = d.regex_except.begin(); ri != d.regex_except.end(); ++ri) {
          int ovector[30]; int rc = pcre_exec(*ri, 0, token, len, 0, 0, ovector, 30);
          if(rc >= 0) { add = 0; break; }
          else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("subset_tee match error");
        }
      }

      if(add) {
        dest.resize(dest.size() + 1);
        dest.back().first = column;
        dest.back().second = (*di).first;
        d.has_data = 1;
        (*di).first->process_token(token, len);
      }
    }
  }
  else {
    for(; di != dest.end() && (*di).first == column; ++di)
      (*di).second->process_token(token, len);
  }

  ++column;
}

template<typename input_base_t> void basic_subset_tee_t<input_base_t>::process_token(double token)
{
  if(first_row) { char buf[32]; size_t len = dtostr(token, buf); process_token(buf, len); return; }

  for(; di != dest.end() && (*di).first == column; ++di)
    (*di).second->process_token(token);

  ++column;
}

template<typename input_base_t> void basic_subset_tee_t<input_base_t>::process_line()
{
  first_row = 0;
  for(typename map<dynamic_pass_t*, dest_data_t>::iterator ddi = dest_data.begin(); ddi != dest_data.end(); ++ddi)
    if((*ddi).second.has_data)
      (*ddi).first->process_line();
  column = 0;
  di = dest.begin();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// ordered_tee
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t> void basic_ordered_tee_t<input_base_t>::reinit_state(int more_passes)
{
  first_row = 1;
  num_columns = 0;
  for(vector<char*>::iterator i = data.begin(); i != data.end(); ++i) delete[] *i;
  data.clear();
  next = 0;
  end = 0;
  if(more_passes == 0) return;
  if(more_passes > 0) --more_passes;
  for(typename vector<dynamic_pass_t*>::iterator i = out.begin(); i != out.end(); ++i)
    (*i)->reinit_state(more_passes);
}

template<typename input_base_t> void basic_ordered_tee_t<input_base_t>::process_token(const char* token, size_t len)
{
  if(!out.size()) throw runtime_error("ordered_tee::process_token no outs");
  out[0]->process_token(token, len);

  if(first_row) ++num_columns;

  if(!next || (len + 2) > size_t(end - next)) {
    if(next) *next++ = '\x03';
    size_t cap = 256 * 1024;
    if(cap < len + 2) cap = len + 2;
    data.push_back(new char[cap]);
    next = data.back();
    end = data.back() + cap;
  } 
  memcpy(next, token, len); next += len;
  *next++ = '\0';
}

template<typename input_base_t> void basic_ordered_tee_t<input_base_t>::process_token(double token)
{
  if(!out.size()) throw runtime_error("ordered_tee::process_token no outs");
  out[0]->process_token(token);

  if(first_row) ++num_columns;

  if(!next || 2 + sizeof(double) > size_t(end - next)) {
    if(next) *next++ = '\x03';
    size_t cap = 256 * 1024;
    data.push_back(new char[cap]);
    next = data.back();
    end = data.back() + cap;
  } 
  *next++ = '\x01';
  memcpy(next, &token, sizeof(double));
  next += sizeof(double);
}

template<typename input_base_t> void basic_ordered_tee_t<input_base_t>::process_line()
{
  if(!out.size()) throw runtime_error("ordered_tee::process_line no outs");

  out[0]->process_line();
  first_row = 0;
}

template<typename input_base_t> void basic_ordered_tee_t<input_base_t>::process_stream()
{
  if(next) *next++ = '\x03';

  vector<dynamic_pass_t*>::iterator oi = out.begin();
  (*oi)->process_stream();
  for(++oi; oi != out.end(); ++oi) {
    vector<dynamic_pass_t*>::iterator noi = oi; ++noi;
    const bool last_out = noi == out.end();

    int column = 0;
    for(vector<char*>::iterator i = data.begin(); i != data.end(); ++i) {
      const char* p = *i;
      while(*p != '\03') {
        if(*p == '\x01') {
          (*oi)->process_token(*reinterpret_cast<const double*>(++p));
          p += sizeof(double);
        }
        else {
          size_t len = strlen(p);
          (*oi)->process_token(p, len);
          p += len + 1;
        }
        if(++column >= num_columns) {
          (*oi)->process_line();
          column = 0;
        }
      }
      if(last_out) delete[] *i;
    }

    if(last_out) { data.clear(); next = 0; }
    (*oi)->process_stream();
  }
}


////////////////////////////////////////////////////////////////////////////////////////////////
// stacker
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t, typename output_base_t> basic_stacker_t<input_base_t, output_base_t>::~basic_stacker_t()
{
  for(typename vector<regex_stack_action_t>::iterator i = regex_actions.begin(); i != regex_actions.end(); ++i) pcre_free((*i).regex);
  for(typename vector<pair<char*, char*> >::iterator i = leave_tokens.begin(); i != leave_tokens.end(); ++i) delete[] (*i).first;
  for(typename vector<pair<char*, char*> >::iterator i = stack_tokens.begin(); i != stack_tokens.end(); ++i) delete[] (*i).first;
}

template<typename input_base_t, typename output_base_t> void basic_stacker_t<input_base_t, output_base_t>::resize(size_t len, vector<pair<char*, char*> >& tokens, size_t& index, char*& next)
{
  if(next) { *next++ = '\x03'; ++index; }
  if(index < tokens.size()) {
    size_t cap = size_t(tokens[index].second - next);
    if(cap < len) {
      cap = len;
      delete[] tokens[index].first;
      tokens[index].first = new char[cap];
      tokens[index].second = tokens[index].first + cap;
    }
  }
  else {
    size_t cap = 256 * 1024;
    if(cap < len) cap = len;
    char* p = new char[cap];
    tokens.push_back(pair<char*, char*>(p, p + cap));
  }
  next = tokens[index].first;
}

template<typename input_base_t, typename output_base_t> void basic_stacker_t<input_base_t, output_base_t>::process_leave_tokens()
{
  vector<pair<char*, char*> >::iterator lti = leave_tokens.begin();
  if(lti == leave_tokens.end()) return;
  for(char* ltp = (*lti).first; 1;) {
    if(*ltp == '\x01') { this->output_token(*reinterpret_cast<double*>(++ltp)); ltp += sizeof(double); }
    else if(*ltp == '\x03') ltp = (*++lti).first;
    else if(*ltp == '\x04') break;
    else { size_t len = strlen(ltp); this->output_token(ltp, len); ltp += len + 1; }
  }
}

template<typename input_base_t, typename output_base_t> void basic_stacker_t<input_base_t, output_base_t>::process_stack_tokens()
{
  vector<pair<char*, char*> >::iterator sti = stack_tokens.begin();
  stack_column = 0;
  for(char* stp = (*sti).first; 1;) {
    process_leave_tokens();
    this->output_token(stack_keys[stack_column].c_str(), stack_keys[stack_column].size());
    if(*stp == '\x01') { this->output_token(*reinterpret_cast<double*>(++stp)); stp += sizeof(double); }
    else { size_t len = strlen(stp); this->output_token(stp, len); stp += len + 1; }
    this->output_line();
    ++stack_column;
    if(*stp == '\x03') stp = (*++sti).first;
    else if(*stp == '\x04') break;
  }
}

template<typename input_base_t, typename output_base_t> void basic_stacker_t<input_base_t, output_base_t>::reinit(int more_passes)
{
  this->default_action = ST_REMOVE;
  keyword_actions.clear();
  for(typename vector<regex_stack_action_t>::iterator i = regex_actions.begin(); i != regex_actions.end(); ++i) pcre_free((*i).regex);
  regex_actions.clear();
  reinit_state();
  this->reinit_output_if(more_passes);
}

template<typename input_base_t, typename output_base_t> void basic_stacker_t<input_base_t, output_base_t>::reinit_state(int more_passes)
{
  first_line = 1;
  stack_keys.clear();
  actions.clear();
  last_leave = 0;

  column = 0;
  stack_column = 0;
  for(vector<pair<char*, char*> >::iterator i = leave_tokens.begin(); i != leave_tokens.end(); ++i) delete[] (*i).first;
  leave_tokens.clear();
  leave_tokens_index = 0;
  leave_tokens_next = 0;
  for(vector<pair<char*, char*> >::iterator i = stack_tokens.begin(); i != stack_tokens.end(); ++i) delete[] (*i).first;
  stack_tokens.clear();
  stack_tokens_index = 0;
  stack_tokens_next = 0;
  this->reinit_output_state_if(more_passes);
}

template<typename input_base_t, typename output_base_t> void basic_stacker_t<input_base_t, output_base_t>::add_action(bool regex, const char* key, stack_action_e action)
{
  if(regex) {
    const char* err; int err_off; pcre* p = pcre_compile(key, 0, &err, &err_off, 0);
    if(!p) throw runtime_error("stacker can't compile group regex");
    regex_actions.resize(regex_actions.size() + 1);
    regex_actions.back().regex = p;
    regex_actions.back().action = action;
  }
  else keyword_actions[key] = action;
}

template<typename input_base_t, typename output_base_t> void basic_stacker_t<input_base_t, output_base_t>::process_token(const char* token, size_t len)
{
  if(first_line) {
    stack_action_e action = default_action;
    string stoken(token, len);
    map<string, stack_action_e>::iterator i = keyword_actions.find(stoken);
    if(i != keyword_actions.end()) action = (*i).second;
    else {
      for(typename vector<regex_stack_action_t>::iterator j = regex_actions.begin(); j != regex_actions.end(); ++j) {
        int ovector[30]; int rc = pcre_exec((*j).regex, 0, token, len, 0, 0, ovector, 30);
        if(rc >= 0) { action = (*j).action; break; }
        else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("stacker match error");
      }
    }
    actions.push_back(action);
    if(action == ST_LEAVE) { last_leave = column; this->output_token(token, len); }
    else if(action == ST_STACK) stack_keys.push_back(stoken);
  }
  else {
    if(column >= actions.size()) throw runtime_error("too many columns");
    else if(actions[column] == ST_LEAVE) {
      if(!leave_tokens_next || leave_tokens_next + len + 2 > leave_tokens[leave_tokens_index].second)
        resize(len + 2, leave_tokens, leave_tokens_index, leave_tokens_next);
      memcpy(leave_tokens_next, token, len); leave_tokens_next += len;
      *leave_tokens_next++ = '\0';
      if(column == last_leave) {
        *leave_tokens_next++ = '\x04';
        if(stack_tokens_next) {
          *stack_tokens_next++ = '\x04';
          process_stack_tokens();
        }
      }
    }
    else if(actions[column] == ST_STACK) {
      if(column < last_leave) {
        if(!stack_tokens_next || stack_tokens_next + len + 2 > stack_tokens[stack_tokens_index].second)
          resize(len + 2, stack_tokens, stack_tokens_index, stack_tokens_next);
        memcpy(stack_tokens_next, token, len); stack_tokens_next += len;
        *stack_tokens_next++ = '\0';
      }
      else {
        process_leave_tokens();
        this->output_token(stack_keys[stack_column].c_str(), stack_keys[stack_column].size());
        this->output_token(token, len);
        this->output_line();
        ++stack_column;
      }
    }
  }

  ++column;
}

template<typename input_base_t, typename output_base_t> void basic_stacker_t<input_base_t, output_base_t>::process_token(double token)
{
  if(first_line) { char buf[32]; size_t len = dtostr(token, buf); process_token(buf, len); return; }

  if(column >= actions.size()) throw runtime_error("too many columns");
  else if(actions[column] == ST_LEAVE) {
    if(!leave_tokens_next || leave_tokens_next + sizeof(token) + 2 > leave_tokens[leave_tokens_index].second)
      resize(sizeof(token) + 2, leave_tokens, leave_tokens_index, leave_tokens_next);
    *leave_tokens_next++ = '\x01';
    memcpy(leave_tokens_next, &token, sizeof(token)); leave_tokens_next += sizeof(token);
    if(column == last_leave) {
      *leave_tokens_next++ = '\x04';
      if(stack_tokens_next) {
        *stack_tokens_next++ = '\x04';
        process_stack_tokens();
      }
    }
  }
  else if(actions[column] == ST_STACK) {
    if(column < last_leave) {
      if(!stack_tokens_next || stack_tokens_next + sizeof(token) + 2 > stack_tokens[stack_tokens_index].second)
        resize(sizeof(token) + 2, stack_tokens, stack_tokens_index, stack_tokens_next);
      *stack_tokens_next++ = '\x01';
      memcpy(stack_tokens_next, &token, sizeof(token)); stack_tokens_next += sizeof(token);
    }
    else {
      process_leave_tokens();
      this->output_token(stack_keys[stack_column].c_str(), stack_keys[stack_column].size());
      this->output_token(token);
      this->output_line();
      ++stack_column;
    }
  }

  ++column;
}

template<typename input_base_t, typename output_base_t> void basic_stacker_t<input_base_t, output_base_t>::process_line()
{
  if(first_line) {
    this->output_token("keyword", 7);
    this->output_token("data", 4);
    this->output_line();
    first_line = 0;
  }
  column = 0;
  stack_column = 0;
  leave_tokens_index = 0;
  leave_tokens_next = 0;
  stack_tokens_index = 0;
  stack_tokens_next = 0;
}

template<typename input_base_t, typename output_base_t> void basic_stacker_t<input_base_t, output_base_t>::process_stream()
{
  for(vector<pair<char*, char*> >::iterator i = leave_tokens.begin(); i != leave_tokens.end(); ++i) delete[] (*i).first;
  leave_tokens.clear();
  for(vector<pair<char*, char*> >::iterator i = stack_tokens.begin(); i != stack_tokens.end(); ++i) delete[] (*i).first;
  stack_tokens.clear();

  this->output_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// splitter
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t, typename output_base_t> basic_splitter_t<input_base_t, output_base_t>::~basic_splitter_t()
{
  for(typename vector<regex_action_t>::const_iterator i = regex_actions.begin(); i != regex_actions.end(); ++i) pcre_free((*i).regex);
  delete[] group_tokens;
  delete[] split_by_tokens;
  delete[] split_tokens;
  for(map<char*, size_t, cstr_less>::const_iterator i = out_split_keys.begin(); i != out_split_keys.end(); ++i) delete[] (*i).first;
  for(vector<char*>::const_iterator i = group_storage.begin(); i != group_storage.end(); ++i) delete[] *i;
}


template<typename input_base_t, typename output_base_t> void basic_splitter_t<input_base_t, output_base_t>::reinit(int more_passes)
{
  this->default_action = SP_REMOVE;
  keyword_actions.clear();
  for(typename vector<regex_action_t>::const_iterator i = regex_actions.begin(); i != regex_actions.end(); ++i) pcre_free((*i).regex);
  regex_actions.clear();
  reinit_state();
  this->reinit_output_if(more_passes);
}

template<typename input_base_t, typename output_base_t> void basic_splitter_t<input_base_t, output_base_t>::reinit_state(int more_passes)
{
  first_line = 1;
  actions.clear();
  group_keys.clear();
  split_keys.clear();
  column = 0;
  delete[] group_tokens;
  group_tokens = new char[2048];
  group_tokens_next = group_tokens;
  group_tokens_end = group_tokens + 2048;
  delete[] split_by_tokens;
  split_by_tokens = new char[2048];
  split_by_tokens_next = split_by_tokens;
  split_by_tokens_end = split_by_tokens + 2048;
  delete[] split_tokens;
  split_tokens = new char[2048];
  split_tokens_next = split_tokens;
  split_tokens_end = split_tokens + 2048;

  for(map<char*, size_t, cstr_less>::const_iterator i = out_split_keys.begin(); i != out_split_keys.end(); ++i) delete[] (*i).first;
  for(vector<char*>::const_iterator i = group_storage.begin(); i != group_storage.end(); ++i) delete[] *i;
  group_storage_next = 0;
  group_storage_end = 0;
  out_split_keys.clear();
  data.clear();
  this->reinit_output_state_if(more_passes);
}

template<typename input_base_t, typename output_base_t> void basic_splitter_t<input_base_t, output_base_t>::add_action(bool regex, const char* key, split_action_e action)
{
  if(regex) {
    const char* err; int err_off; pcre* p = pcre_compile(key, 0, &err, &err_off, 0);
    if(!p) throw runtime_error("splitter can't compile data regex");
    regex_actions.resize(regex_actions.size() + 1);
    regex_actions.back().regex = p;
    regex_actions.back().action = action;
  }
  else keyword_actions[key] = action;
}

template<typename input_base_t, typename output_base_t> void basic_splitter_t<input_base_t, output_base_t>::process_token(const char* token, size_t len)
{
  if(first_line) {
    split_action_e action = default_action;
    string stoken(token, len);
    map<string, split_action_e>::iterator i = keyword_actions.find(stoken);
    if(i != keyword_actions.end()) action = (*i).second;
    else {
      for(typename vector<regex_action_t>::iterator j = regex_actions.begin(); j != regex_actions.end(); ++j) {
        int ovector[30]; int rc = pcre_exec((*j).regex, 0, token, len, 0, 0, ovector, 30);
        if(rc >= 0) { action = (*j).action; break; }
        else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("splitter match error");
      }
    }
    actions.push_back(action);
    if(action == SP_GROUP) group_keys.push_back(stoken);
    else if(action == SP_SPLIT) split_keys.push_back(stoken);
  }
  else {
    if(actions[column] == SP_GROUP) {
      if(group_tokens_next + len >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end, len + 1);
      memcpy(group_tokens_next, token, len); group_tokens_next += len;
      *group_tokens_next++ = '\0';
    }
    else if(actions[column] == SP_SPLIT_BY) {
      if(split_by_tokens_next + len >= split_by_tokens_end) resize_buffer(split_by_tokens, split_by_tokens_next, split_by_tokens_end, len + 1);
      memcpy(split_by_tokens_next, token, len); split_by_tokens_next += len;
      *split_by_tokens_next++ = '\0';
    }
    else if(actions[column] == SP_SPLIT) {
      if(split_tokens_next + len >= split_tokens_end) resize_buffer(split_tokens, split_tokens_next, split_tokens_end, len + 1);
      memcpy(split_tokens_next, token, len); split_tokens_next += len;
      *split_tokens_next++ = '\0';
    }
  }

  ++column;
}

template<typename input_base_t, typename output_base_t> void basic_splitter_t<input_base_t, output_base_t>::process_line()
{
  if(first_line) {
    first_line = 0;
  }
  else {
    if(group_tokens_next >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end);
    *group_tokens_next++ = '\x03';
    if(split_by_tokens_next >= split_by_tokens_end) resize_buffer(split_by_tokens, split_by_tokens_next, split_by_tokens_end);
    *split_by_tokens_next++ = '\x03';
    if(split_tokens_next >= split_tokens_end) resize_buffer(split_tokens, split_tokens_next, split_tokens_end);
    *split_tokens_next++ = '\x03';

    data_t::iterator di = data.find(group_tokens);
    if(di == data.end()) {
      size_t len = group_tokens_next - group_tokens;
      if(len + 1 > size_t(group_storage_end - group_storage_next)) {
        if(group_storage_next) *group_storage_next++ = '\x04';
        size_t cap = 256 * 1024;
        if(cap < len + 1) cap = len + 1;
        group_storage.push_back(new char[cap]);
        group_storage_next = group_storage.back();
        group_storage_end = group_storage.back() + cap;
      }
      memcpy(group_storage_next, group_tokens, len);
      di = data.insert(data_t::value_type(group_storage_next, vector<string>())).first;
      group_storage_next += len;
    }
    vector<string>& values = (*di).second;
    vector<string>::const_iterator ski = split_keys.begin();
    for(char* stp = split_tokens; *stp != '\x03'; ++ski, ++stp) {
      group_tokens_next = group_tokens;
      for(const char* p = (*ski).c_str(); *p; ++p) {
        if(group_tokens_next >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end);
        *group_tokens_next++ = *p;
      }
      for(char* sbtp = split_by_tokens; *sbtp != '\x03'; ++sbtp) {
        *group_tokens_next++ = ' ';
        while(*sbtp) {
          if(group_tokens_next >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end);
          *group_tokens_next++ = *sbtp++;
        }
      }
      *group_tokens_next++ = '\0';

      map<char*, size_t, cstr_less>::iterator i = out_split_keys.find(group_tokens);
      if(i == out_split_keys.end()) {
        char* cpy = new char[group_tokens_next - group_tokens];
        memcpy(cpy, group_tokens, group_tokens_next - group_tokens);
        i = out_split_keys.insert(map<char*, size_t, cstr_less>::value_type(cpy, out_split_keys.size())).first;
      }
      const size_t& index = (*i).second;
      if(index >= values.size()) values.resize(index + 1);
      values[index] = stp;

      while(*stp) ++stp;
    }
  }

  column = 0;
  group_tokens_next = group_tokens;
  split_by_tokens_next = split_by_tokens;
  split_tokens_next = split_tokens;
}

template<typename input_base_t, typename output_base_t> void basic_splitter_t<input_base_t, output_base_t>::process_stream()
{
  for(vector<string>::const_iterator i = group_keys.begin(); i != group_keys.end(); ++i)
    this->output_token((*i).c_str(), (*i).size());
  {
    vector<char*> osk(out_split_keys.size());
    for(map<char*, size_t, cstr_less>::const_iterator i = out_split_keys.begin(); i != out_split_keys.end(); ++i)
      osk[(*i).second] = (*i).first;
    for(vector<char*>::const_iterator i = osk.begin(); i != osk.end(); ++i)
      this->output_token(*i, strlen(*i));
  }
  this->output_line();

  map<char*, size_t, cstr_less>::size_type num_out_split_keys = out_split_keys.size();
  for(map<char*, size_t, cstr_less>::const_iterator i = out_split_keys.begin(); i != out_split_keys.end(); ++i)
    delete[] (*i).first;
  out_split_keys.clear();

  for(data_t::const_iterator i = data.begin(); i != data.end(); ++i) {
    for(char* p = (*i).first; *p != '\x03';) {
      size_t len = strlen(p);
      this->output_token(p, len);
      p += len + 1;
    }
    size_t tokens = 0;
    for(vector<string>::const_iterator j = (*i).second.begin(); j != (*i).second.end(); ++j, ++tokens)
      this->output_token((*j).c_str(), (*j).size());
    while(tokens++ < num_out_split_keys)
      this->output_token("", 0);
    this->output_line();
  }

  for(vector<char*>::const_iterator i = group_storage.begin(); i != group_storage.end(); ++i) delete[] *i;
  group_storage.clear();
  data.clear();

  this->output_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// sorter
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t, typename output_base_t> bool basic_sorter_t<input_base_t, output_base_t>::compare::operator() (const basic_sorter_t::row_t& lhs, const basic_sorter_t::row_t& rhs)
{
  typename vector<sorts_t>::const_iterator i = sorts.begin();
  const char* l = lhs.sort;
  const char* r = rhs.sort;

  while(1) {
    uint8_t type = (*i).type;

    while(*l == *r && *l != '\0') { ++l; ++r; }

    if(*l < *r) return type == 1 ? 1 : 0;
    else if(*l > *r) return type == 1 ? 0 : 1;
    else { //both at null terminator
      if(*++l == '\x03') break;
      else ++r;
    }

    ++i;
  }

  return 0;
}

template<typename input_base_t, typename output_base_t> basic_sorter_t<input_base_t, output_base_t>::~basic_sorter_t() {
  delete[] sort_buf_end;
  if(sort_buf) { for(size_t i = 0; i < sorts.size(); ++i) delete[] sort_buf[i]; }
  delete[] sort_buf;
  for(vector<char*>::const_iterator i = sort_storage.begin(); i != sort_storage.end(); ++i) delete[] *i;
  for(vector<char*>::const_iterator i = other_storage.begin(); i != other_storage.end(); ++i) delete[] *i;
}

template<typename input_base_t, typename output_base_t> void basic_sorter_t<input_base_t, output_base_t>::reinit_state(int more_passes)
{
  columns.clear();
  sorts_found = 0;
  first_line = 1;
  delete[] sort_buf_end; sort_buf_end = 0;
  if(sort_buf) { for(size_t i = 0; i < sorts.size(); ++i) delete[] sort_buf[i]; }
  delete[] sort_buf; sort_buf = 0;
  for(vector<char*>::const_iterator i = sort_storage.begin(); i != sort_storage.end(); ++i) delete[] *i;
  sort_storage.clear();
  sort_next = 0;
  sort_end = 0;
  for(vector<char*>::const_iterator i = other_storage.begin(); i != other_storage.end(); ++i) delete[] *i;
  other_storage.clear();
  other_next = 0;
  other_end = 0;
  rows.clear();
  this->reinit_output_state_if(more_passes);
}

template<typename input_base_t, typename output_base_t> void basic_sorter_t<input_base_t, output_base_t>::add_sort(const char* key, bool ascending)
{
  for(typename vector<sorts_t>::const_iterator i = sorts.begin(); i != sorts.end(); ++i)
    if(!(*i).key.compare(key))
      throw runtime_error("sorter has sort already");
  sorts.resize(sorts.size() + 1);
  sorts.back().key = key;
  sorts.back().type = ascending ? 1 : 2;
}

template<typename input_base_t, typename output_base_t> void basic_sorter_t<input_base_t, output_base_t>::process_token(const char* token, size_t len)
{
  size_t index;

  if(first_line) {
    if(!sort_buf) {
      sort_buf = new char*[sorts.size()];
      sort_buf_end = new char*[sorts.size()];
      for(size_t i = 0; i < sorts.size(); ++i) {
        sort_buf[i] = new char[16];
        *sort_buf[i] = '\0';
        sort_buf_end[i] = sort_buf[i] + 16;
      }
      sort_buf_len = 0;
    }

    index = numeric_limits<size_t>::max();
    for(size_t i = 0; i < sorts.size(); ++i) {
      if(!sorts[i].key.compare(token)) { index = i; ++sorts_found; break; }
    }
    columns.push_back(index);
  }
  else {
    if(ci == columns.begin()) {
      rows.resize(rows.size() + 1);
      rows.back().sort = sort_next;
      rows.back().other = other_next;
      rows.back().other_index = other_storage.size() ? other_storage.size() - 1 : 0;
    }
    index = *ci++;
  }

  if(index == numeric_limits<size_t>::max()) {
    if(other_next + len + 2 >= other_end) {
      if(other_storage.size() && other_next == other_storage.back()) {
        size_t row_off = rows.size() ? rows.back().other - other_storage.back() : 0;
        resize_buffer(other_storage.back(), other_next, other_end, len + 2);
        if(rows.size()) rows.back().other = other_next + row_off;
      }
      else {
        if(other_next) *other_next = '\x04';
        size_t cap = len + 2;
        if(cap < 256 * 1024) cap = 256 * 1024;
        other_storage.resize(other_storage.size() + 1);
        other_storage.back() = new char[cap];
        other_next = other_storage.back();
        other_end = other_storage.back() + cap;
        if(rows.size() && !rows.back().other) rows.back().other = other_next;
      }
    }
    memcpy(other_next, token, len); other_next += len;
    *other_next++ = '\0';
  }
  else {
    char* next = sort_buf[index];
    if(sort_buf[index] + len >= sort_buf_end[index]) resize_buffer(sort_buf[index], next, sort_buf_end[index], len + 1);
    memcpy(next, token, len); next += len;
    *next++ = '\0';
    sort_buf_len += len;
  }
}

template<typename input_base_t, typename output_base_t> void basic_sorter_t<input_base_t, output_base_t>::process_line()
{
  if(first_line) {
    if(sorts_found < sorts.size()) throw runtime_error("sorter didn't find enough columns");
    if(sorts_found > sorts.size()) throw runtime_error("sorter didn't find too many sort columns");
    first_line = 0;

    for(size_t i = 0; i < sorts.size(); ++i) {
      this->output_token(sort_buf[i], strlen(sort_buf[i]));
      sort_buf[i][0] = '\0';
    }

    *other_next++ = '\x03';
    vector<char*>::iterator osi = other_storage.begin();
    char* p = osi == other_storage.end() ? 0 : *osi;
    while(1) {
      const char* s = p;
      while(*p) ++p;
      this->output_token(s, p - s);
      ++p;
      if(*p == '\x03') { delete[] *osi; break; }
      else if(*p == '\x04') {
        delete[] *osi; *osi = 0;
        ++osi;
        p = osi == other_storage.end() ? 0 : *osi;
      }
    }
    other_storage.clear();
    other_next = 0;
    other_end = 0;
    this->output_line();
  }
  else {
    if(!sort_storage.size() || sort_storage.back() + sort_buf_len + sorts.size() + 2 > sort_end) {
      if(sort_storage.size() && rows.back().sort == sort_storage.back()) {
        size_t row_off = rows.back().sort - sort_storage.back();
        resize_buffer(sort_storage.back(), sort_next, sort_end, sort_buf_len + sorts.size() + 2);
        rows.back().sort = sort_next + row_off;
      }
      else {
        if(sort_next) *sort_next = '\x04';
        sort_storage.resize(sort_storage.size() + 1);
        size_t cap = sort_buf_len + sorts.size() + 2;
        if(cap < 256 * 1024) cap = 256 * 1024;
        sort_storage.back() = new char[cap];
        sort_next = sort_storage.back();
        sort_end = sort_storage.back() + cap;
        if(!rows.back().sort) rows.back().sort = sort_next;
      }
    }
    for(size_t i = 0; i < sorts.size(); ++i) {
      char* p = sort_buf[i];
      while(*p) *sort_next++ = *p++;
      *sort_next++ = '\0';
    }
    *sort_next++ = '\x03';
    *other_next++ = '\x03';
    for(size_t i = 0; i < sorts.size(); ++i) { *sort_buf[i] = '\0'; }
  }

  ci = columns.begin();
}

template<typename input_base_t, typename output_base_t> void basic_sorter_t<input_base_t, output_base_t>::process_stream()
{
  if(sort_next) *sort_next++ = '\x04';
  if(other_next) *other_next++ = '\x04';

  compare comp(sorts);
  sort(rows.begin(), rows.end(), comp);

  for(typename vector<row_t>::iterator ri = rows.begin(); ri != rows.end(); ++ri) {
    for(const char* p = (*ri).sort; *p != '\x03';) {
      size_t len = strlen(p);
      this->output_token(p, len);
      p += len + 1;
    }
    size_t index = (*ri).other_index;
    for(const char* p = (*ri).other; *p != '\x03';) {
      size_t len = strlen(p);
      this->output_token(p, len);
      p += len + 1;
      if(*p == '\x04') {
        p = other_storage[++index];
      }
    }
    this->output_line();
  }

  columns.clear();
  delete[] sort_buf_end; sort_buf_end = 0;
  if(sort_buf) { for(size_t i = 0; i < sorts.size(); ++i) delete[] sort_buf[i]; }
  delete[] sort_buf; sort_buf = 0;
  for(vector<char*>::const_iterator i = sort_storage.begin(); i != sort_storage.end(); ++i) delete[] *i;
  sort_storage.clear();
  sort_next = 0;
  sort_end = 0;
  for(vector<char*>::const_iterator i = other_storage.begin(); i != other_storage.end(); ++i) delete[] *i;
  other_storage.clear();
  other_next = 0;
  other_end = 0;

  this->output_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// row_joiner
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t, typename output_base_t> basic_row_joiner_t<input_base_t, output_base_t>::~basic_row_joiner_t()
{
  for(typename vector<data_t>::iterator i = data.begin(); i != data.end(); ++i)
    for(vector<char*>::iterator j = (*i).data.begin(); j != (*i).data.end(); ++j)
      delete[] *j;
}

template<typename input_base_t, typename output_base_t> void basic_row_joiner_t<input_base_t, output_base_t>::reinit_state(int more_passes)
{
  table = 0;
  first_line = 1;
  more_lines = 0;
  column = 0;
  fill(num_columns.begin(), num_columns.end(), 0);
  keys.clear();
  for(typename vector<data_t>::iterator i = data.begin(); i != data.end(); ++i)
    for(vector<char*>::iterator j = (*i).data.begin(); j != (*i).data.end(); ++j)
      delete[] *j;
  data.clear();
  this->reinit_output_state_if(more_passes);
}

template<typename input_base_t, typename output_base_t> void basic_row_joiner_t<input_base_t, output_base_t>::add_table_name(const char* name)
{
  if(name) table_name.push_back(name);
  else {
    stringstream ss; ss << "Table " << table_name.size();
    table_name.push_back(ss.str());
  }
  num_columns.push_back(0);
}

template<typename input_base_t, typename output_base_t> void basic_row_joiner_t<input_base_t, output_base_t>::process_token(const char* token, size_t len)
{
  if(!first_line) {
    data_t& d = data[table];
    if(!d.next || (len + 2) > size_t(d.end - d.next)) {
      if(d.next) *d.next++ = '\x03';
      size_t cap = 256 * 1024;
      if(cap < len + 2) cap = len + 2;
      d.data.push_back(new char[cap]);
      d.next = d.data.back();
      d.end = d.data.back() + cap;
    }
    memcpy(d.next, token, len + 1);
    d.next += len + 1;
  }
  else {
    string stoken(token, len);
    if(!more_lines) {
      if(table >= table_name.size()) add_table_name();
      ++num_columns[table];
      keys.push_back(stoken);
    }
    else {
      size_t c = column;
      for(size_t t = 0; t < table; ++t)
        c += num_columns[t];
      if(keys[c].compare(token) && keys[c].compare(stoken + " of " + table_name[table]))
        throw runtime_error("column keys don't match previous tables");
      ++column;
    }
  }
}

template<typename input_base_t, typename output_base_t> void basic_row_joiner_t<input_base_t, output_base_t>::process_token(double token)
{
  if(first_line) { char buf[32]; size_t len = dtostr(token, buf); process_token(buf, len); return; }

  data_t& d = data[table];
  if(!d.next || (sizeof(double) + 2) > size_t(d.end - d.next)) {
    if(d.next) *d.next++ = '\x03';
    size_t cap = 256 * 1024;
    d.data.push_back(new char[cap]);
    d.next = d.data.back();
    d.end = d.data.back() + cap;
  }
  *d.next++ = '\x01';
  memcpy(d.next, &token, sizeof(double));
  d.next += sizeof(double);
}

template<typename input_base_t, typename output_base_t> void basic_row_joiner_t<input_base_t, output_base_t>::process_line()
{
  if(first_line) {
    first_line = 0;
    data.resize(data.size() + 1);
    data.back().next = 0;
    data.back().end = 0;
    if(more_lines) {
      if(column != num_columns[table]) throw runtime_error("num_columns doesn't match");
      column = 0;
    }
  }
}

template<typename input_base_t, typename output_base_t> void basic_row_joiner_t<input_base_t, output_base_t>::process_stream() { ++table; first_line = 1; }

template<typename input_base_t, typename output_base_t> void basic_row_joiner_t<input_base_t, output_base_t>::process_lines()
{
  if(!more_lines) {
    for(vector<string>::iterator ki = keys.begin(); ki != keys.end(); ++ki) {
      bool found = 0;
      vector<string>::iterator nki = ki; ++nki;
      while(1) {
        nki = find(nki, keys.end(), *ki);
        if(nki == keys.end()) break;

        found = 1;
        size_t keyi = nki - keys.begin();
        size_t t = 0;
        for(; t < num_columns.size(); ++t) {
          if(keyi < num_columns[t]) break;
          else keyi -= num_columns[t];
        }
        (*nki) = (*nki) + " of " + table_name[t];
      }

      if(found) {
        size_t keyi = ki - keys.begin();
        size_t t = 0;
        for(; t < num_columns.size(); ++t) {
          if(keyi < num_columns[t]) break;
          else keyi -= num_columns[t];
        }
        (*ki) = (*ki) + " of " + table_name[t];
      }
      this->output_token((*ki).c_str(), (*ki).size());
    }
    this->output_line();
  }

  for(typename vector<data_t>::iterator di = data.begin(); di != data.end(); ++di) {
    data_t& d = *di;
    d.i = d.data.begin();
    if(d.i == d.data.end()) d.next = 0;
    else {
      *d.next++ = '\x03';
      d.next = *d.i;
    }
  }

  bool done = 0;
  while(!done) { //line loop
    vector<size_t>::const_iterator nci = num_columns.begin();
    for(typename vector<data_t>::iterator di = data.begin(); di != data.end(); ++di, ++nci) { // table loop
      data_t& d = *di;
      for(size_t col = 0; col < (*nci); ++col) {
        if(*d.next == '\x03') {
          if(++d.i != d.data.end()) d.next = *d.i;
          else { d.next = 0; done = 1; break; }
        }
        if(*d.next == '\x01') {
          this->output_token(*reinterpret_cast<const double*>(++d.next));
          d.next += sizeof(double);
        }
        else {
          size_t len = strlen(d.next);
          this->output_token(d.next, len);
          d.next += len + 1;
        }
      }
    }
    if(!done) this->output_line();
  }

  table = 0;
  first_line = 1;
  more_lines = 1;
  for(typename vector<data_t>::iterator i = data.begin(); i != data.end(); ++i)
    for(vector<char*>::iterator j = (*i).data.begin(); j != (*i).data.end(); ++j)
      delete[] *j;
  data.clear();
}

template<typename input_base_t, typename output_base_t> void basic_row_joiner_t<input_base_t, output_base_t>::process()
{
  if(data.size()) process_lines();

  this->output_stream();
  table_name.clear();
  num_columns.clear();
  keys.clear();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// col_pruner
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t, typename output_base_t> basic_col_pruner_t<input_base_t, output_base_t>::~basic_col_pruner_t()
{
  delete[] has_data;
  for(vector<char*>::iterator i = data.begin(); i != data.end(); ++i) delete[] *i;
}

template<typename input_base_t, typename output_base_t> void basic_col_pruner_t<input_base_t, output_base_t>::reinit_state(int more_passes)
{
  first_row = 1;
  passthrough = 0;
  column = 0;
  delete[] has_data; has_data = 0;
  for(vector<char*>::iterator i = data.begin(); i != data.end(); ++i) delete[] *i;
  data.clear();
  next = 0;
  this->reinit_output_state_if(more_passes);
}

template<typename input_base_t, typename output_base_t> void basic_col_pruner_t<input_base_t, output_base_t>::process_token(const char* token, size_t len)
{
  if(passthrough) { this->output_token(token, len); return; }

  if(!first_row && token[0]) {
    if(!(has_data[column / 32] & (1 << (column % 32)))) {
      has_data[column / 32] |= 1 << (column % 32);
      ++columns_with_data;
    }
  }

  if(!next || (len + 2) > size_t(end - next)) {
    if(next) *next++ = '\x03';
    size_t cap = 256 * 1024;
    if(cap < len + 2) cap = len + 2;
    data.push_back(new char[cap]);
    next = data.back();
    end = data.back() + cap;
  } 
  memcpy(next, token, len); next += len;
  *next++ = '\0';
  ++column;
}

template<typename input_base_t, typename output_base_t> void basic_col_pruner_t<input_base_t, output_base_t>::process_token(double token)
{
  if(passthrough) { this->output_token(token); return; }

  if(!first_row && !isnan(token)) {
    if(!(has_data[column / 32] & (1 << (column % 32)))) {
      has_data[column / 32] |= 1 << (column % 32);
      ++columns_with_data;
    }
  }

  if(!next || (sizeof(double) + 2) > size_t(end - next)) {
    if(next) *next++ = '\x03';
    size_t cap = 256 * 1024;
    data.push_back(new char[cap]);
    next = data.back();
    end = data.back() + cap;
  } 
  *next++ = '\x01';
  memcpy(next, &token, sizeof(double));
  next += sizeof(double);
  ++column;
}

template<typename input_base_t, typename output_base_t> void basic_col_pruner_t<input_base_t, output_base_t>::process_line()
{
  if(passthrough) { this->output_line(); return; }

  if(first_row) {
    first_row = 0;
    num_columns = column;
    columns_with_data = 0;
    const size_t has_data_words = (num_columns + 31) / 32;
    delete[] has_data; has_data = new uint32_t[has_data_words];
    memset(has_data, 0x00, has_data_words * sizeof(uint32_t));
  }
  else if(columns_with_data >= num_columns) {
    if(next) *next++ = '\x03';
    size_t c = 0;
    for(vector<char*>::iterator i = data.begin(); i != data.end(); ++i) {
      const char* p = *i;
      while(*p != '\03') {
        if(*p == '\x01') {
          this->output_token(*reinterpret_cast<const double*>(++p));
          p += sizeof(double);
        }
        else {
          size_t len = strlen(p);
          this->output_token(p, len);
          p += len + 1;;
        }
        if(++c >= num_columns) {
          this->output_line();
          c = 0;
        }
      }
      delete[] *i;
    }
    passthrough = 1;
    delete[] has_data; has_data = 0;
    data.clear();
    next = 0;
  }

  column = 0;
}

template<typename input_base_t, typename output_base_t> void basic_col_pruner_t<input_base_t, output_base_t>::process_stream()
{
  if(passthrough) { this->output_stream(); return; }

  if(next) *next++ = '\x03';

  size_t c = 0;
  for(vector<char*>::iterator i = data.begin(); i != data.end(); ++i) {
    const char* p = *i;
    while(*p != '\03') {
      if(has_data[c / 32] & (1 << (c % 32))) {
        if(*p == '\x01') {
          this->output_token(*reinterpret_cast<const double*>(++p));
          p += sizeof(double);
        }
        else {
          size_t len = strlen(p);
          this->output_token(p, len);
          p += len + 1;;
        }
      }
      else {
        if(*p == '\x01') { p += 1 + sizeof(double); }
        else {
          while(*p) ++p;
          ++p;
        }
      }
      if(++c >= num_columns) {
        this->output_line();
        c = 0;
      }
    }
    delete[] *i;
  }
  delete[] has_data; has_data = 0;
  data.clear();
  next = 0;
  this->output_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// combiner
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t, typename output_base_t> void basic_combiner_t<input_base_t, output_base_t>::add_pair(const char* from, const char* to)
{
  const char* err; int err_off; pcre* p = pcre_compile(from, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("combiner can't compile regex");
  pairs.resize(pairs.size() + 1);
  pairs.back().first = p;
  pairs.back().second = to;
}

template<typename input_base_t, typename output_base_t> void basic_combiner_t<input_base_t, output_base_t>::process_token(const char* token, size_t len)
{
  if(first_row) {
    tokens.push_back(string(token, len));
  }
  else if(token[0]) {
    const int index = remap_indexes[column];
    if(!tokens[index].size()) tokens[index] = token;
    else throw runtime_error("multiple values for combiner");
  }

  ++column;
}

template<typename input_base_t, typename output_base_t> void basic_combiner_t<input_base_t, output_base_t>::process_line()
{
  if(first_row) {
    //create remap_indexes with gaps
    remap_indexes.resize(tokens.size());
    for(size_t i = 0; i < remap_indexes.size(); ++i) remap_indexes[i] = i;

    char* buf = new char[2048];
    char* end = buf + 2048;
    for(vector<pair<pcre*, string> >::iterator i = pairs.begin(); i != pairs.end(); ++i) {
      for(size_t j = 0; j < tokens.size(); ++j) {
        int ovector[30]; int rc = pcre_exec((*i).first, 0, tokens[j].c_str(), tokens[j].size(), 0, 0, ovector, 30);
        if(rc < 0) { if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("combiner match error"); }
        else {
          char* next = buf;
          if(!rc) rc = 10;
          generate_substitution(tokens[j].c_str(), (*i).second.c_str(), ovector, rc, buf, next, end);

          size_t k = 0;
          for(; k < tokens.size(); ++k) {
            if(j == k) continue;
            if(!tokens[k].compare(buf)) break;
          }
          if(k >= tokens.size()) {
            stringstream msg; msg << "can't find to (" << buf << ')';
            throw runtime_error(msg.str());
          }
          remap_indexes[j] = k;
          tokens[j] = buf;
        }
      }
    }
    delete[] buf;

    //compact out the gaps
    int max_out_index = 0;
    for(size_t i = 0; i < remap_indexes.size(); ++i)
      if(max_out_index < remap_indexes[i])
        max_out_index = remap_indexes[i];

    for(int i = 0; i <= max_out_index;) {
      vector<int>::iterator it = find(remap_indexes.begin(), remap_indexes.end(), i);
      if(it != remap_indexes.end()) { ++i; continue; }

      for(size_t j = 0; j < remap_indexes.size(); ++j) if(remap_indexes[j] > i) --remap_indexes[j];
      --max_out_index;
    }

    //print header
    for(int i = 0; i <= max_out_index; ++i) {
      vector<int>::iterator it = find(remap_indexes.begin(), remap_indexes.end(), i);
      if(it == remap_indexes.end()) throw runtime_error("wth");
      this->output_token(tokens[distance(remap_indexes.begin(), it)].c_str(), tokens[distance(remap_indexes.begin(), it)].size());
    }

    //setup for second line
    tokens.resize(max_out_index + 1);
    for(vector<string>::iterator i = tokens.begin(); i != tokens.end(); ++i) (*i).clear();
    first_row = 0;
  }
  else {
    for(vector<string>::iterator i = tokens.begin(); i != tokens.end(); ++i) {
      this->output_token((*i).c_str(), (*i).size());
      (*i).clear();
    }
  }
  this->output_line();
  column = 0;
}


////////////////////////////////////////////////////////////////////////////////////////////////
// summarizer
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t, typename output_base_t> basic_summarizer_t<input_base_t, output_base_t>::~basic_summarizer_t()
{
  for(vector<pcre*>::iterator gri = pre_sorted_group_regexes.begin(); gri != pre_sorted_group_regexes.end(); ++gri) pcre_free(*gri);
  for(vector<pcre*>::iterator gri = group_regexes.begin(); gri != group_regexes.end(); ++gri) pcre_free(*gri);
  for(vector<pair<pcre*, uint32_t> >::iterator dri = data_regexes.begin(); dri != data_regexes.end(); ++dri) pcre_free((*dri).first);
  for(vector<pcre*>::iterator ei = exception_regexes.begin(); ei != exception_regexes.end(); ++ei) pcre_free(*ei);
  delete[] values;
  delete[] pre_sorted_group_tokens;
  delete[] group_tokens;
  delete[] pre_sorted_group_storage;
  for(vector<char*>::iterator i = group_storage.begin(); i != group_storage.end(); ++i) delete[] *i;
  for(typename vector<data_t*>::iterator i = data_storage.begin(); i != data_storage.end(); ++i) delete[] *i;
}

template<typename input_base_t, typename output_base_t> void basic_summarizer_t<input_base_t, output_base_t>::print_header(char*& buf, char*& next, char*& end, const char* op, size_t op_len, const char* token, size_t len)
{
  if(next + op_len + len + 3 > end) resize_buffer(buf, next, end, op_len + len + 3);
  memcpy(next, op, op_len); next += op_len;
  *next++ = '(';
  memcpy(next, token, len); next += len;
  *next++ = ')';
  *next++ = '\0';
}

template<typename input_base_t, typename output_base_t> void basic_summarizer_t<input_base_t, output_base_t>::print_data()
{
  if(!data.size()) return;

  *group_storage_next++ = '\x04';
  data.clear();

  size_t data_rows_per_storage = (256 * 1024) / (sizeof(data_t) * num_data_columns);
  if(!data_rows_per_storage) data_rows_per_storage = 1;

  size_t row = 0;
  vector<char*>::const_iterator gsi = group_storage.begin();
  char* g = (gsi == group_storage.end()) ? 0 : *gsi;
  typename vector<data_t*>::const_iterator dsi = data_storage.begin();
  data_t* d = (dsi == data_storage.end()) ? 0 : *dsi;
  while(g && d) {
    if(pre_sorted_group_storage) {
      char* pg = pre_sorted_group_storage;
      while(*pg != '\x03') { size_t len = strlen(pg); this->output_token(pg, len); pg += len + 1; }
    }
    while(*g != '\x03') { size_t len = strlen(g); this->output_token(g, len); g += len + 1; }
    ++g;
    if(*g == '\x04') { delete[] *gsi; ++gsi; g = (gsi == group_storage.end()) ? 0 : *gsi; }

    for(cfi = column_flags.begin(); cfi != column_flags.end(); ++cfi) {
      if(!((*cfi) & 0xFFFFFFFC)) continue;

      if((*cfi) & SUM_MISSING) { this->output_token((*d).missing); }
      if((*cfi) & SUM_COUNT) { this->output_token((*d).count); }
      if((*cfi) & SUM_SUM) { this->output_token((*d).count ? (*d).sum : numeric_limits<double>::quiet_NaN()); }
      if((*cfi) & SUM_MIN) { this->output_token((*d).count ? (*d).min : numeric_limits<double>::quiet_NaN()); }
      if((*cfi) & SUM_MAX) { this->output_token((*d).count ? (*d).max : numeric_limits<double>::quiet_NaN()); }
      if((*cfi) & SUM_AVG) { this->output_token((*d).count ? ((*d).sum / (*d).count) : numeric_limits<double>::quiet_NaN()); }
      if((*cfi) & (SUM_VARIANCE | SUM_STD_DEV)) {
        if((*d).count > 1) {
          double v = (*d).sum_of_squares - ((*d).sum * (*d).sum) / (*d).count;
          v /= (*d).count - 1;
          if((*cfi) & SUM_VARIANCE) { this->output_token(v); }
          if((*cfi) & SUM_STD_DEV) { this->output_token(sqrt(v)); }
        }
        else if((*d).count == 1) {
          if((*cfi) & SUM_VARIANCE) { this->output_token(0.0); }
          if((*cfi) & SUM_STD_DEV) { this->output_token(0.0); }
        }
        else {
          if((*cfi) & SUM_VARIANCE) { this->output_token(numeric_limits<double>::quiet_NaN()); }
          if((*cfi) & SUM_STD_DEV) { this->output_token(numeric_limits<double>::quiet_NaN()); }
        }
      }
      ++d;
    }
    if((row % data_rows_per_storage) == (data_rows_per_storage - 1)) {
      delete[] *dsi; ++dsi; d = (dsi == data_storage.end()) ? 0 : *dsi;
    }
    ++row;

    this->output_line();
  }

  for(; gsi != group_storage.end(); ++gsi) delete[] *gsi;
  group_storage.clear();
  group_storage_next = 0;
  for(; dsi != data_storage.end(); ++dsi) delete[] *dsi;
  data_storage.clear();
  data_storage_next = 0;
}

template<typename input_base_t, typename output_base_t> void basic_summarizer_t<input_base_t, output_base_t>::reinit(int more_passes)
{
  for(vector<pcre*>::iterator gri = group_regexes.begin(); gri != group_regexes.end(); ++gri) pcre_free(*gri);
  group_regexes.clear();
  for(vector<pair<pcre*, uint32_t> >::iterator dri = data_regexes.begin(); dri != data_regexes.end(); ++dri) pcre_free((*dri).first);
  data_regexes.clear();
  for(vector<pcre*>::iterator ei = exception_regexes.begin(); ei != exception_regexes.end(); ++ei) pcre_free(*ei);
  exception_regexes.clear();
  reinit_state();
  this->reinit_output_if(more_passes);
}

template<typename input_base_t, typename output_base_t> void basic_summarizer_t<input_base_t, output_base_t>::reinit_state(int more_passes)
{
  first_line = 1;
  column_flags.clear();
  num_data_columns = 0;
  delete[] values; values = 0;
  delete[] pre_sorted_group_tokens; pre_sorted_group_tokens = new char[2048];
  pre_sorted_group_tokens_next = pre_sorted_group_tokens;
  pre_sorted_group_tokens_end = pre_sorted_group_tokens + 2048;
  delete[] group_tokens; group_tokens = new char[2048];
  group_tokens_next = group_tokens;
  group_tokens_end = group_tokens + 2048;
  delete[] pre_sorted_group_storage; pre_sorted_group_storage = new char[2048];
  *pre_sorted_group_storage = '\x03';
  pre_sorted_group_storage_end = pre_sorted_group_storage + 2048;
  for(vector<char*>::iterator i = group_storage.begin(); i != group_storage.end(); ++i) delete[] *i;
  group_storage.clear();
  group_storage_next = 0;
  for(typename vector<data_t*>::iterator i = data_storage.begin(); i != data_storage.end(); ++i) delete[] *i;
  data_storage.clear();
  data_storage_next = 0;
  data.clear();
  this->reinit_output_state_if(more_passes);
}

template<typename input_base_t, typename output_base_t> void basic_summarizer_t<input_base_t, output_base_t>::add_group(const char* regex, bool pre_sorted)
{
  const char* err; int err_off; pcre* p = pcre_compile(regex, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("summarizer can't compile group regex");
  if(pre_sorted) pre_sorted_group_regexes.push_back(p);
  else group_regexes.push_back(p);
}

template<typename input_base_t, typename output_base_t> void basic_summarizer_t<input_base_t, output_base_t>::add_data(const char* regex, uint32_t flags)
{
  const char* err; int err_off; pcre* p = pcre_compile(regex, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("summarizer can't compile data regex");
  data_regexes.push_back(pair<pcre*, uint32_t>(p, flags & 0xFFFFFFFC));
}

template<typename input_base_t, typename output_base_t> void basic_summarizer_t<input_base_t, output_base_t>::add_exception(const char* regex)
{
  const char* err; int err_off; pcre* p = pcre_compile(regex, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("summarizer can't compile exception regex");
  exception_regexes.push_back(p);
}

template<typename input_base_t, typename output_base_t> void basic_summarizer_t<input_base_t, output_base_t>::process_token(const char* token, size_t len)
{
  if(first_line) {
    uint32_t flags = 0;
    for(vector<pcre*>::iterator gri = pre_sorted_group_regexes.begin(); gri != pre_sorted_group_regexes.end(); ++gri) {
      int ovector[30]; int rc = pcre_exec(*gri, 0, token, len, 0, 0, ovector, 30);
      if(rc >= 0) { flags = 1; break; }
      else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("summarizer match error");
    }
    if(!flags) {
      for(vector<pcre*>::iterator gri = group_regexes.begin(); gri != group_regexes.end(); ++gri) {
        int ovector[30]; int rc = pcre_exec(*gri, 0, token, len, 0, 0, ovector, 30);
        if(rc >= 0) { flags = 2; break; }
        else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("summarizer match error");
      }
    }
    for(vector<pair<pcre*, uint32_t> >::iterator dri = data_regexes.begin(); dri != data_regexes.end(); ++dri) {
      int ovector[30]; int rc = pcre_exec((*dri).first, 0, token, len, 0, 0, ovector, 30);
      if(rc >= 0) { flags |= (*dri).second; }
      else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("summarizer match error");
    }
    if(flags) {
      for(vector<pcre*>::iterator ei = exception_regexes.begin(); ei != exception_regexes.end(); ++ei) {
        int ovector[30]; int rc = pcre_exec(*ei, 0, token, len, 0, 0, ovector, 30);
        if(rc >= 0) { flags = 0; break; }
        else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("summarizer match error");
      }
    }

    if(flags & 1) { this->output_token(token, len); }
    if(flags & 2) {
      if(group_tokens_next + len >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end, len + 1);
      memcpy(group_tokens_next, token, len); group_tokens_next += len;
      *group_tokens_next++ = '\0';
    }
    if(flags & SUM_MISSING) { print_header(pre_sorted_group_tokens, pre_sorted_group_tokens_next, pre_sorted_group_tokens_end, "MISSING", 7, token, len); }
    if(flags & SUM_COUNT) { print_header(pre_sorted_group_tokens, pre_sorted_group_tokens_next, pre_sorted_group_tokens_end, "COUNT", 5, token, len); }
    if(flags & SUM_SUM) { print_header(pre_sorted_group_tokens, pre_sorted_group_tokens_next, pre_sorted_group_tokens_end, "SUM", 3, token, len); }
    if(flags & SUM_MIN) { print_header(pre_sorted_group_tokens, pre_sorted_group_tokens_next, pre_sorted_group_tokens_end, "MIN", 3, token, len); }
    if(flags & SUM_MAX) { print_header(pre_sorted_group_tokens, pre_sorted_group_tokens_next, pre_sorted_group_tokens_end, "MAX", 3, token, len); }
    if(flags & SUM_AVG) { print_header(pre_sorted_group_tokens, pre_sorted_group_tokens_next, pre_sorted_group_tokens_end, "AVG", 3, token, len); }
    if(flags & SUM_VARIANCE) { print_header(pre_sorted_group_tokens, pre_sorted_group_tokens_next, pre_sorted_group_tokens_end, "VARIANCE", 8, token, len); }
    if(flags & SUM_STD_DEV) { print_header(pre_sorted_group_tokens, pre_sorted_group_tokens_next, pre_sorted_group_tokens_end, "STD_DEV", 7, token, len); }

    column_flags.push_back(flags);
    if(flags & 0xFFFFFFFC) ++num_data_columns;
  }
  else {
    const uint32_t& flags = *cfi;
    if(flags & 1) {
      if(pre_sorted_group_tokens_next + len >= pre_sorted_group_tokens_end) resize_buffer(pre_sorted_group_tokens, pre_sorted_group_tokens_next, pre_sorted_group_tokens_end, len + 1);
      memcpy(pre_sorted_group_tokens_next, token, len); pre_sorted_group_tokens_next += len;
      *pre_sorted_group_tokens_next++ = '\0';
    }
    else if(flags & 2) {
      if(group_tokens_next + len >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end, len + 1);
      memcpy(group_tokens_next, token, len); group_tokens_next += len;
      *group_tokens_next++ = '\0';
    }
    if(flags & 0xFFFFFFFC) {
      if(!len) { *vi = numeric_limits<double>::quiet_NaN(); }
      else { *vi = strtod(token, 0); }
      ++vi;
    }
    ++cfi;
  }
}

template<typename input_base_t, typename output_base_t> void basic_summarizer_t<input_base_t, output_base_t>::process_token(double token)
{
  if(first_line) { char buf[32]; size_t len = dtostr(token, buf); process_token(buf, len); return; }

  const uint32_t& flags = *cfi;
  if(flags & 1) {
    if(pre_sorted_group_tokens_next + 31 >= pre_sorted_group_tokens_end) resize_buffer(pre_sorted_group_tokens, pre_sorted_group_tokens_next, pre_sorted_group_tokens_end, 32);
    pre_sorted_group_tokens_next += dtostr(token, pre_sorted_group_tokens_next) + 1;
  }
  else if(flags & 2) {
    if(group_tokens_next + 31 >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end, 32);
    group_tokens_next += dtostr(token, group_tokens_next) + 1;
  }
  if(flags & 0xFFFFFFFC) { *vi++ = token; }
  ++cfi;
}

template<typename input_base_t, typename output_base_t> void basic_summarizer_t<input_base_t, output_base_t>::process_line()
{
  if(first_line) {
    if(!num_data_columns) throw runtime_error("summarizer has no data columns");
    for(char* p = group_tokens; p < group_tokens_next; ++p) {
      size_t len = strlen(p);
      this->output_token(p, len);
      p += len;
    }
    for(char* p = pre_sorted_group_tokens; p < pre_sorted_group_tokens_next; ++p) { // data column headers
      size_t len = strlen(p);
      this->output_token(p, len);
      p += len;
    }
    this->output_line();
    first_line = 0;
    values = new double[num_data_columns];
  }
  else {
    if(pre_sorted_group_tokens_next != pre_sorted_group_tokens) {
      if(pre_sorted_group_tokens_next >= pre_sorted_group_tokens_end) resize_buffer(pre_sorted_group_tokens, pre_sorted_group_tokens_next, pre_sorted_group_tokens_end);
      *pre_sorted_group_tokens_next++ = '\x03';
      multi_cstr_equal_to e;
      if(!e(pre_sorted_group_tokens, pre_sorted_group_storage)) {
        const size_t len = pre_sorted_group_tokens_next - pre_sorted_group_tokens;
        print_data();
        if(size_t(pre_sorted_group_storage_end - pre_sorted_group_storage) < len) {
          delete[] pre_sorted_group_storage;
          pre_sorted_group_storage = new char[pre_sorted_group_tokens_end - pre_sorted_group_tokens];
        }
        memcpy(pre_sorted_group_storage, pre_sorted_group_tokens, len);
      }
    }
    if(group_tokens_next >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end);
    *group_tokens_next++ = '\x03';
    typename data_map_t::iterator i = data.find(group_tokens);
    if(i == data.end()) {
      size_t len = group_tokens_next - group_tokens;
      if(!group_storage_next || len >= size_t(group_storage_end - group_storage_next)) {
        if(group_storage_next) *group_storage_next++ = '\04';
        size_t cap = 256 * 1024;
        if(cap < len + 1) cap = len + 1;
        group_storage.push_back(new char[cap]);
        group_storage_next = group_storage.back();
        group_storage_end = group_storage.back() + cap;
      }
      memcpy(group_storage_next, group_tokens, len);
      if(!data_storage_next || num_data_columns > size_t(data_storage_end - data_storage_next)) {
        size_t rows = (256 * 1024) / (sizeof(data_t) * num_data_columns);
        if(!rows) rows = 1;
        data_storage.push_back(new data_t[rows * num_data_columns]);
        data_storage_next = data_storage.back();
        data_storage_end = data_storage.back() + (rows * num_data_columns);
      }
      i = data.insert(typename data_map_t::value_type(group_storage_next, data_storage_next)).first;
      group_storage_next += len;
      data_storage_next += num_data_columns;
    }
    vi = values;
    for(size_t c = 0; c < num_data_columns; ++c, ++vi) {
      data_t& d = (*i).second[c];
      if(isnan(*vi)) ++d.missing;
      else {
        ++d.count;
        d.sum += *vi;
        d.sum_of_squares += *vi * *vi;
        if(*vi < d.min) d.min = *vi;
        if(*vi > d.max) d.max = *vi;
      }
    }
  }

  cfi = column_flags.begin();
  vi = values;
  pre_sorted_group_tokens_next = pre_sorted_group_tokens;
  group_tokens_next = group_tokens;
}


////////////////////////////////////////////////////////////////////////////////////////////////
// range_stacker
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t, typename output_base_t> void basic_range_stacker_t<input_base_t, output_base_t>::reinit_state(int more_passes)
{
  first_row = 1;
  column = 0;
  columns.clear();
  for(vector<pair<char*, char*> >::iterator i = leave_tokens.begin(); i != leave_tokens.end(); ++i) delete[] (*i).first;
  leave_tokens.clear();
  leave_tokens_index = 0;
  leave_tokens_next = 0;
  this->reinit_output_if(more_passes);
}

template<typename input_base_t, typename output_base_t> void basic_range_stacker_t<input_base_t, output_base_t>::add(const char* start, const char* stop, const char* new_name)
{
  ranges.resize(ranges.size() + 1);
  ranges.back().start_name = start;
  ranges.back().start_col_index = numeric_limits<size_t>::max();
  ranges.back().stop_name = stop;
  ranges.back().stop_col_index = numeric_limits<size_t>::max();
  ranges.back().new_col_name = new_name;
}

template<typename input_base_t, typename output_base_t> void basic_range_stacker_t<input_base_t, output_base_t>::process_token(const char* token, size_t len)
{
  if(first_row) {
    int match = 0;
    for(typename vector<range_t>::iterator i = ranges.begin(); i != ranges.end(); ++i) {
      if(!(*i).start_name.compare(0, string::npos, token, len)) { match = 1; (*i).start_col_index = columns.size(); }
      else if(!(*i).stop_name.compare(0, string::npos, token, len)) { match = 1; (*i).stop_col_index = columns.size(); }
    }
    if(match) {
      columns.resize(columns.size() + 1);
      columns.back().col = column;
    }
    else this->output_token(token, len);
  }
  else {
    if(ci != columns.end() && (*ci).col == column) {
      char* next = 0; (*ci).val = strtod(token, &next);
      if(next == token) (*ci).val = numeric_limits<double>::quiet_NaN();
      ++ci;
    }
    else {
      if(!leave_tokens_next || leave_tokens_next + len + 2 > leave_tokens[leave_tokens_index].second) {
        if(leave_tokens_next) { *leave_tokens_next++ = '\x03'; ++leave_tokens_index; }
        if(leave_tokens_index < leave_tokens.size()) {
          size_t cap = size_t(leave_tokens[leave_tokens_index].second - leave_tokens_next);
          if(cap < len + 2) {
            cap = len + 2;
            delete[] leave_tokens[leave_tokens_index].first;
            leave_tokens[leave_tokens_index].first = new char[cap];
            leave_tokens[leave_tokens_index].second = leave_tokens[leave_tokens_index].first + cap;
          }
        }
        else {
          size_t cap = 256 * 1024;
          if(cap < len + 2) cap = len + 2;
          char* p = new char[cap];
          leave_tokens.push_back(pair<char*, char*>(p, p + cap));
        }
        leave_tokens_next = leave_tokens[leave_tokens_index].first;
      }
      memcpy(leave_tokens_next, token, len + 1);
      leave_tokens_next += len + 1;
    }
  }

  ++column;
}

template<typename input_base_t, typename output_base_t> void basic_range_stacker_t<input_base_t, output_base_t>::process_token(double token)
{
  if(first_row) { char buf[32]; size_t len = dtostr(token, buf); process_token(buf, len); return; }

  if(ci != columns.end() && (*ci).col == column) {
    (*ci).val = token;
    ++ci;
  }
  else {
    if(!leave_tokens_next || leave_tokens_next + sizeof(double) + 2 > leave_tokens[leave_tokens_index].second) {
      if(leave_tokens_next) { *leave_tokens_next++ = '\x03'; ++leave_tokens_index; }
      if(leave_tokens_index >= leave_tokens.size()) {
        size_t cap = 256 * 1024;
        char* p = new char[cap];
        leave_tokens.push_back(pair<char*, char*>(p, p + cap));
      }
      leave_tokens_next = leave_tokens[leave_tokens_index].first;
    }
    *leave_tokens_next++ = '\x01';
    memcpy(leave_tokens_next, &token, sizeof(double));
    leave_tokens_next += sizeof(double);
  }

  ++column;
}

template<typename input_base_t, typename output_base_t> void basic_range_stacker_t<input_base_t, output_base_t>::process_line() {
  if(first_row) {
    first_row = 0;
    for(typename vector<range_t>::const_iterator i = ranges.begin(); i != ranges.end(); ++i) {
      if((*i).start_col_index >= column) throw runtime_error("range_stacker missing start");
      else if((*i).stop_col_index >= column) throw runtime_error("range_stacker missing stop");
      this->output_token((*i).new_col_name.c_str(), (*i).new_col_name.size());
    }
    this->output_line();
  }
  else {
    *leave_tokens_next++ = '\x04';
    bool done = 1;
    for(typename vector<range_t>::iterator i = ranges.begin(); i != ranges.end(); ++i) {
      (*i).cur_val = columns[(*i).start_col_index].val;

      if((*i).cur_val <= columns[(*i).stop_col_index].val) done = 0;
      else (*i).cur_val = numeric_limits<double>::quiet_NaN();
    }
    while(!done) {
      vector<pair<char*, char*> >::iterator lti = leave_tokens.begin();
      if(lti != leave_tokens.end()) {
        for(char* ltp = (*lti).first; 1;) {
          if(*ltp == '\x01') { this->output_token(*reinterpret_cast<double*>(++ltp)); ltp += sizeof(double); }
          else if(*ltp == '\x03') ltp = (*++lti).first;
          else if(*ltp == '\x04') break;
          else { size_t len = strlen(ltp); this->output_token(ltp, len); ltp += len + 1; }
        }
      }
      for(typename vector<range_t>::iterator i = ranges.begin(); i != ranges.end(); ++i)
        this->output_token((*i).cur_val);
      this->output_line();

      done = 1;
      for(typename vector<range_t>::reverse_iterator i = ranges.rbegin(); i != ranges.rend(); ++i) {
        if(isnan((*i).cur_val)) continue;
        (*i).cur_val += 1.0;
        if((*i).cur_val > columns[(*i).stop_col_index].val) (*i).cur_val = columns[(*i).start_col_index].val;
        else { done = 0; break; }
      }
    }
  }

  column = 0;
  ci = columns.begin();
  leave_tokens_index = 0;
  leave_tokens_next = 0;
}

template<typename input_base_t, typename output_base_t> void basic_range_stacker_t<input_base_t, output_base_t>::process_stream()
{
  for(vector<pair<char*, char*> >::iterator i = leave_tokens.begin(); i != leave_tokens.end(); ++i) delete[] (*i).first;
  leave_tokens.clear();
  columns.clear();
  this->output_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// base_converter
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t, typename output_base_t> void basic_base_converter_t<input_base_t, output_base_t>::add_conv(const char* regex, int from, int to)
{
  if(from != 8 && from != 10 && from != 16) throw runtime_error("from isn't recognized");
  if(to != 8 && to != 10 && to != 16) throw runtime_error("from isn't recognized");

  const char* err; int err_off; pcre* p = pcre_compile(regex, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("base_converter can't compile regex");
  regex_base_conv.resize(regex_base_conv.size() + 1);
  regex_base_conv.back().regex = p;
  regex_base_conv.back().from = from;
  regex_base_conv.back().to = to;
}

template<typename input_base_t, typename output_base_t> void basic_base_converter_t<input_base_t, output_base_t>::process_token(const char* token, size_t len)
{
  if(first_row) {
    conv_t c; c.from = -1; c.to = 0;
    for(typename vector<regex_base_conv_t>::const_iterator i = regex_base_conv.begin(); i != regex_base_conv.end(); ++i) {
      int ovector[30]; int rc = pcre_exec((*i).regex, 0, token, len, 0, 0, ovector, 30);
      if(rc >= 0) { c.from = (*i).from; c.to = (*i).to; break; }
      else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("base_converter match error");
    }
    conv.push_back(c);
    this->output_token(token, len);
  }
  else if(conv[column].from < 0) this->output_token(token, len);
  else {
    char* next = 0;
    long int ivalue = 0;
    double dvalue = 0.0;
    if(conv[column].from == 10) { dvalue = strtod(token, &next); ivalue = (long int)dvalue; }
    else { ivalue = strtol(token, &next, conv[column].from); dvalue = ivalue; }

    if(next == token) this->output_token(token, len);
    else {
      if(conv[column].to == 10) { this->output_token(dvalue); }
      else if(conv[column].to == 8) { char buf[256]; int len = sprintf(buf, "%#lo", ivalue); this->output_token(buf, len); }
      else if(conv[column].to == 16) { char buf[256]; int len = sprintf(buf, "%#lx", ivalue); this->output_token(buf, len); }
      else this->output_token(token, len);
    }
  }

  ++column;
}

template<typename input_base_t, typename output_base_t> void basic_base_converter_t<input_base_t, output_base_t>::process_token(double token)
{
  if(first_row) { char buf[32]; size_t len = dtostr(token, buf); process_token(buf, len); return; }

  if(conv[column].to == 8) { char buf[256]; int len = sprintf(buf, "%#lo", (long int)token); this->output_token(buf, len); }
  else if(conv[column].to == 16) { char buf[256]; int len = sprintf(buf, "%#lx", (long int)token); this->output_token(buf, len); }
  else this->output_token(token);

  ++column;
}


////////////////////////////////////////////////////////////////////////////////////////////////
// variance_analyzer
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t, typename output_base_t> basic_variance_analyzer_t<input_base_t, output_base_t>::~basic_variance_analyzer_t()
{
  for(vector<pcre*>::iterator i = group_regexes.begin(); i != group_regexes.end(); ++i) pcre_free(*i);
  for(vector<pcre*>::iterator i = data_regexes.begin(); i != data_regexes.end(); ++i) pcre_free(*i);
  for(vector<pcre*>::iterator i = exception_regexes.begin(); i != exception_regexes.end(); ++i) pcre_free(*i);
  delete[] group_tokens;
  delete[] values;
  for(typename vector<treatment_data_t*>::iterator i = data.begin(); i != data.end(); ++i) delete[] *i;
}

template<typename input_base_t, typename output_base_t> void basic_variance_analyzer_t<input_base_t, output_base_t>::reinit(int more_passes)
{
  for(vector<pcre*>::iterator i = group_regexes.begin(); i != group_regexes.end(); ++i) pcre_free(*i);
  group_regexes.clear();
  for(vector<pcre*>::iterator i = data_regexes.begin(); i != data_regexes.end(); ++i) pcre_free(*i);
  data_regexes.clear();
  for(vector<pcre*>::iterator i = exception_regexes.begin(); i != exception_regexes.end(); ++i) pcre_free(*i);
  exception_regexes.clear();
  reinit_state();
  this->reinit_output_if(more_passes);
}

template<typename input_base_t, typename output_base_t> void basic_variance_analyzer_t<input_base_t, output_base_t>::reinit_state(int more_passes)
{
  first_line = 1;
  column_type.clear();
  delete[] group_tokens; group_tokens = new char[2048];
  group_tokens_next = group_tokens;
  group_tokens_end = group_tokens + 2048;
  group_storage.clear();
  group_storage_next = 0;
  groups.clear();
  delete[] values; values = 0;
  for(typename vector<treatment_data_t*>::iterator i = data.begin(); i != data.end(); ++i) delete[] *i;
  data.clear();
  this->reinit_output_state_if(more_passes);
}

template<typename input_base_t, typename output_base_t> void basic_variance_analyzer_t<input_base_t, output_base_t>::add_group(const char* regex)
{
  const char* err; int err_off; pcre* p = pcre_compile(regex, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("variance_analyzer can't compile group regex");
  group_regexes.push_back(p);
}

template<typename input_base_t, typename output_base_t> void basic_variance_analyzer_t<input_base_t, output_base_t>::add_data(const char* regex)
{
  const char* err; int err_off; pcre* p = pcre_compile(regex, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("variance_analyzer can't compile data regex");
  data_regexes.push_back(p);
}

template<typename input_base_t, typename output_base_t> void basic_variance_analyzer_t<input_base_t, output_base_t>::add_exception(const char* regex)
{
  const char* err; int err_off; pcre* p = pcre_compile(regex, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("variance_analyzer can't compile exception regex");
  exception_regexes.push_back(p);
}

template<typename input_base_t, typename output_base_t> void basic_variance_analyzer_t<input_base_t, output_base_t>::process_token(const char* token, size_t len)
{
  if(first_line) {
    char type = 0;
    for(vector<pcre*>::iterator j = group_regexes.begin(); j != group_regexes.end(); ++j) {
      int ovector[30]; int rc = pcre_exec(*j, 0, token, len, 0, 0, ovector, 30);
      if(rc >= 0) { type = 1; break; }
      else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("variance_analyzer match error");
    }
    if(!type) {
      for(vector<pcre*>::iterator j = data_regexes.begin(); j != data_regexes.end(); ++j) {
        int ovector[30]; int rc = pcre_exec(*j, 0, token, len, 0, 0, ovector, 30);
        if(rc >= 0) { type = 2; break; }
        else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("variance_analyzer match error");
      }
    }
    if(type) {
      for(vector<pcre*>::iterator j = exception_regexes.begin(); j != exception_regexes.end(); ++j) {
        int ovector[30]; int rc = pcre_exec(*j, 0, token, len, 0, 0, ovector, 30);
        if(rc >= 0) { type = 0; break; }
        else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("variance_analyzer match error");
      }
    }
    column_type.push_back(type);
    if(type == 2) data_keywords.push_back(string(token, len));
  }
  else {
    if(cti == column_type.end()) return;

    if(*cti == 1) {
      if(group_tokens_next + len >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end, len + 1);
      memcpy(group_tokens_next, token, len); group_tokens_next += len;
      *group_tokens_next++ = '\0';
    }
    else if(*cti == 2) {
      char* next; *vi = strtod(token, &next);
      if(next == token) *vi = numeric_limits<double>::quiet_NaN();
      ++vi;
    }
    ++cti;
  }
}

template<typename input_base_t, typename output_base_t> void basic_variance_analyzer_t<input_base_t, output_base_t>::process_token(double token)
{
  if(first_line) { char buf[32]; size_t len = dtostr(token, buf); process_token(buf, len); return; }

  if(cti == column_type.end()) return;

  if(*cti == 1) {
    if(group_tokens_next + 31 >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end, 32);
    group_tokens_next += dtostr(token, group_tokens_next) + 1;
  }
  else if(*cti == 2) { *vi++ = token; }
  ++cti;
}

template<typename input_base_t, typename output_base_t> void basic_variance_analyzer_t<input_base_t, output_base_t>::process_line()
{
  if(first_line) {
    first_line = 0;
    values = new double[data_keywords.size()];
  }
  else {
    if(group_tokens_next >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end);
    *group_tokens_next++ = '\x03';

    groups_t::iterator gi = groups.find(group_tokens);
    if(gi == groups.end()) {
      size_t len = group_tokens_next - group_tokens;
      if(!group_storage_next || len + 1 > size_t(group_storage_end - group_storage_next)) {
        if(group_storage_next) *group_storage_next++ = '\x04';
        size_t cap = 256 * 1024;
        if(cap < len + 1) cap = len + 1;
        group_storage.push_back(new char[cap]);
        group_storage_next = group_storage.back();
        group_storage_end = group_storage.back() + cap;
      }
      memcpy(group_storage_next, group_tokens, len);
      gi = groups.insert(groups_t::value_type(group_storage_next, groups.size())).first;
      group_storage_next += len;
      data.push_back(new treatment_data_t[data_keywords.size()]);
    }
    vi = values;
    treatment_data_t* d = data[(*gi).second];
    for(size_t i = 0; i < data_keywords.size(); ++i, ++vi, ++d) {
      if(!isnan(*vi)) {
        ++d->count;
        d->sum += *vi;
        d->sum_of_squares += *vi * *vi;
      }
    }
  }

  cti = column_type.begin();
  group_tokens_next = group_tokens;
  vi = values;
}

template<typename input_base_t, typename output_base_t> void basic_variance_analyzer_t<input_base_t, output_base_t>::process_stream()
{
  if(group_storage_next) *group_storage_next++ = '\x04';

  this->output_token("keyword", 7);
  vector<char*>::const_iterator gsi = group_storage.begin();
  char* gsp = gsi == group_storage.end() ? 0 : *gsi;
  while(gsp) {
    group_tokens_next = group_tokens + 8;
    for(; 1; ++gsp) {
      if(!*gsp) { *group_tokens_next++ = ' '; }
      else if(*gsp == '\x03') { --group_tokens_next; *group_tokens_next++ = ')'; *group_tokens_next = '\0'; break; }
      else { *group_tokens_next++ = *gsp; }
    }
    memcpy(group_tokens + 4, "AVG(", 4); this->output_token(group_tokens + 4, group_tokens_next - group_tokens - 4);
    memcpy(group_tokens, "STD_DEV(", 8); this->output_token(group_tokens, group_tokens_next - group_tokens);
    if(*++gsp == '\x04') { ++gsi; gsp = gsi == group_storage.end() ? 0 : *gsi; }
  }
  this->output_token("AVG", 3);
  this->output_token("STD_DEV", 7);
  this->output_token("f", 1);
  this->output_token("p", 1);
  this->output_line();

  const size_t max_groups = groups.size();
  for(gsi = group_storage.begin(); gsi != group_storage.end(); ++gsi) delete[] *gsi;
  groups.clear();

  size_t di = 0;
  for(vector<string>::const_iterator dki = data_keywords.begin(); dki != data_keywords.end(); ++dki, ++di) {
    this->output_token((*dki).c_str(), (*dki).size());

    size_t num_groups = max_groups;
    int total_count = 0;
    double total_sum = 0.0;
    double sum_of_sum_of_squares = 0.0;
    double sum_of_sum_squared_over_count = 0.0;
    for(size_t gi = 0; gi < max_groups; ++gi) {
      treatment_data_t* dp = data[gi] + di;

      if(!dp->count) --num_groups;
      total_count += dp->count;
      total_sum += dp->sum;
      sum_of_sum_of_squares += dp->sum_of_squares;
      sum_of_sum_squared_over_count += (dp->sum * dp->sum) / dp->count;

      if(dp->count) this->output_token(dp->sum / dp->count);
      else this->output_token(numeric_limits<double>::quiet_NaN());
      if(dp->count > 1) this->output_token(sqrt((dp->sum_of_squares - ((dp->sum * dp->sum) / dp->count)) / (dp->count - 1)));
      else this->output_token(numeric_limits<double>::quiet_NaN());
    }

    if(num_groups > 1) {
      double total_sum_squared_over_total_count = ((total_sum * total_sum) / total_count);
      double sst = sum_of_sum_of_squares - total_sum_squared_over_total_count;
      //double sst_df = (total_count - 1);
      double sstr = sum_of_sum_squared_over_count - total_sum_squared_over_total_count;
      double sstr_df = (num_groups - 1);
      double sse = sst - sstr;
      double sse_df = (total_count - num_groups);
      double f = (sstr / sstr_df) / (sse / sse_df);
      if(isnan(f))
        f = numeric_limits<double>::infinity();
      double p = 1.0;
      if(f != numeric_limits<double>::infinity())
        p = ibeta(sstr_df / 2, sse_df / 2, (sstr_df * f) / (sstr_df * f + sse_df));

      this->output_token(total_sum / total_count);
      this->output_token(sqrt(sst / (total_count - 1)));
      this->output_token(f);
      this->output_token(p);
    }
    else {
      this->output_token(numeric_limits<double>::quiet_NaN());
      this->output_token(numeric_limits<double>::quiet_NaN());
      this->output_token(numeric_limits<double>::quiet_NaN());
      this->output_token(numeric_limits<double>::quiet_NaN());
    }

    this->output_line();
  }

  for(typename data_t::iterator di = data.begin(); di != data.end(); ++di) delete[] *di;
  data.clear();

  this->output_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// unary_col_adder
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t, typename output_base_t, typename op_in_t, typename op_out_t, typename op_t>
void basic_unary_col_adder_t<input_base_t, output_base_t, op_in_t, op_out_t, op_t>::add(const char* regex, const char* new_key, const op_t& op, bool remove_source)
{
  const char* err; int err_off; pcre* p = pcre_compile(regex, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("basic_unary_col_adder can't compile regex");
  insts.resize(insts.size() + 1);
  insts.back().regex = p;
  insts.back().remove_source = remove_source;
  insts.back().new_key = new_key;
  insts.back().op = op;
}

template<typename input_base_t, typename output_base_t, typename op_in_t, typename op_out_t, typename op_t>
void basic_unary_col_adder_t<input_base_t, output_base_t, op_in_t, op_out_t, op_t>::process_token(const char* token, size_t len)
{
  if(first_row) {
    bool matched = 0;
    bool remove_source = 0;
    for(typename vector<inst_t>::iterator i = insts.begin(); i != insts.end(); ++i) {
      int ovector[30]; int rc = pcre_exec((*i).regex, 0, token, len, 0, 0, ovector, 30);
      if(rc < 0) { if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("basic_unary_col_adder match error"); }
      else {
        if(!matched) {
          columns.resize(columns.size() + 1);
          columns.back().col = column;
          columns.back().passthrough = 1;
          matched = 1;
        }
        if((*i).remove_source) { columns.back().passthrough = 0; remove_source = 1; }
        columns.back().ops.push_back((*i).op);
        char* next = buf;
        if(!rc) rc = 10;
        generate_substitution(token, (*i).new_key.c_str(), ovector, rc, buf, next, end);
        this->output_token(buf, next - buf - 1);
      }
    }
    if(!remove_source) this->output_token(token, len);
  }
  else {
    if(ci != columns.end() && column == (*ci).col) {
      for(typename vector<op_t>::iterator oi = (*ci).ops.begin(); oi != (*ci).ops.end(); ++oi) {
        op_in_t in = get_in_value(token, len, (op_in_t*)0);
        op_out_t val = (*oi)(in);
        this->output_token(val);
      }
      if((*ci).passthrough) this->output_token(token, len);
      ++ci;
    }
    else this->output_token(token, len);
  }

  ++column;
}

template<typename input_base_t, typename output_base_t, typename op_in_t, typename op_out_t, typename op_t>
void basic_unary_col_adder_t<input_base_t, output_base_t, op_in_t, op_out_t, op_t>::process_token(double token)
{
  if(first_row) { char buf[32]; size_t len = dtostr(token, buf); process_token(buf, len); return; }

  if(ci != columns.end() && column == (*ci).col) {
    for(typename vector<op_t>::iterator oi = (*ci).ops.begin(); oi != (*ci).ops.end(); ++oi) {
      char buf[32];
      op_in_t in = get_in_value(token, buf, (op_in_t*)0);
      op_out_t val = (*oi)(in);
      this->output_token(val);
    }
    if((*ci).passthrough) this->output_token(token);
    ++ci;
  }
  else this->output_token(token);

  ++column;
}


////////////////////////////////////////////////////////////////////////////////////////////////
// binary_col_adder
////////////////////////////////////////////////////////////////////////////////////////////////

template<typename input_base_t, typename output_base_t, typename op_in1_t, typename op_in2_t, typename op_out_t, typename op_t>
basic_binary_col_adder_t<input_base_t, output_base_t, op_in1_t, op_in2_t, op_out_t, op_t>::~basic_binary_col_adder_t()
{
  for(typename vector<inst_t>::iterator i = insts.begin(); i != insts.end(); ++i) pcre_free((*i).regex);
  for(vector<char*>::const_iterator i = key_storage.begin(); i != key_storage.end(); ++i) delete[] *i;
  for(ci = columns.begin(); ci != columns.end(); ++ci) { delete[] (*ci).c_str_val.c_str; }
}

template<typename input_base_t, typename output_base_t, typename op_in1_t, typename op_in2_t, typename op_out_t, typename op_t>
void basic_binary_col_adder_t<input_base_t, output_base_t, op_in1_t, op_in2_t, op_out_t, op_t>::reinit_state(int more_passes)
{
  first_row = 1;
  column = 0;
  for(vector<char*>::const_iterator i = key_storage.begin(); i != key_storage.end(); ++i) delete[] *i;
  key_storage.clear();
  key_storage_next = 0;
  key_storage_end = 0;
  keys.clear();
  for(ci = columns.begin(); ci != columns.end(); ++ci) { delete[] (*ci).c_str_val.c_str; }
  columns.clear();
  new_columns.clear();
  this->reinit_output_if(more_passes);
}

template<typename input_base_t, typename output_base_t, typename op_in1_t, typename op_in2_t, typename op_out_t, typename op_t>
void basic_binary_col_adder_t<input_base_t, output_base_t, op_in1_t, op_in2_t, op_out_t, op_t>::add(const char* regex, const char* other_key, const char* new_key, const op_t& op, bool remove_source, bool remove_other)
{
  const char* err; int err_off; pcre* p = pcre_compile(regex, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("basic_binary_col_adder can't compile regex");
  insts.resize(insts.size() + 1);
  insts.back().regex = p;
  insts.back().remove_source = remove_source;
  insts.back().other_key = other_key;
  insts.back().remove_other = remove_other;
  insts.back().new_key = new_key;
  insts.back().op = op;
}

template<typename input_base_t, typename output_base_t, typename op_in1_t, typename op_in2_t, typename op_out_t, typename op_t>
void basic_binary_col_adder_t<input_base_t, output_base_t, op_in1_t, op_in2_t, op_out_t, op_t>::process_token(const char* token, size_t len)
{
  if(first_row) {
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
    if(ci != columns.end() && (*ci).col == column) {
      col_t& c = *ci;
      if(c.need_double) {
        char* next; c.double_val = strtod(token, &next);
        if(next == token) c.double_val = numeric_limits<double>::quiet_NaN();
      }
      if(c.need_c_str) {
        if(!c.c_str_val.c_str || c.c_str_val.c_str + len >= c.c_str_end) {
          const size_t cap = ((len + 1) * 150) / 100;
          c.c_str_val.c_str = new char[cap];
          c.c_str_end = c.c_str_val.c_str + cap;
        }
        char* next = (char*)c.c_str_val.c_str;
        memcpy(next, token, len); next += len;
        *next = '\0';
        c.c_str_val.len = len;
      }
      if(c.passthrough) this->output_token(token, len);
      ++ci;
    }
    else this->output_token(token, len);
  }

  ++column;
}

template<typename input_base_t, typename output_base_t, typename op_in1_t, typename op_in2_t, typename op_out_t, typename op_t>
void basic_binary_col_adder_t<input_base_t, output_base_t, op_in1_t, op_in2_t, op_out_t, op_t>::process_token(double token)
{
  if(first_row) { char buf[32]; size_t len = dtostr(token, buf); process_token(buf, len); return; }

  if(ci != columns.end() && (*ci).col == column) {
    col_t& c = *ci;
    if(c.need_double) { c.double_val = token; }
    if(c.need_c_str) {
      if(!c.c_str_val.c_str || c.c_str_val.c_str + 32 > c.c_str_end) {
        c.c_str_val.c_str = new char[32];
        c.c_str_end = c.c_str_val.c_str + 32;
      }
      c.c_str_val.len = dtostr(token, (char*)c.c_str_val.c_str);
    }
    if(c.passthrough) this->output_token(token);
    ++ci;
  }
  else this->output_token(token);

  ++column;
}

template<typename input_base_t, typename output_base_t, typename op_in1_t, typename op_in2_t, typename op_out_t, typename op_t>
void basic_binary_col_adder_t<input_base_t, output_base_t, op_in1_t, op_in2_t, op_out_t, op_t>::process_line()
{
  if(first_row) {
    char* buf = new char[2048];
    char* end = buf + 2048;
    size_t* lens = new size_t[keys.size()];
    map<size_t, col_info_t> cols;
    map<size_t, vector<new_col_info_t> > new_cols;

    for(column = 0; column < keys.size(); ++column) {
      lens[column] = strlen(keys[column]);
      for(typename vector<inst_t>::iterator ii = insts.begin(); ii != insts.end(); ++ii) {
        int ovector[30]; int rc = pcre_exec((*ii).regex, 0, keys[column], lens[column], 0, 0, ovector, 30);
        if(rc < 0){ if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("basic_binary_col_adder match error"); }
        else {
          char* next = buf;
          if(!rc) rc = 10;
          generate_substitution(keys[column], (*ii).other_key.c_str(), ovector, rc, buf, next, end);
          size_t other_col = 0;
          while(other_col < keys.size() && strcmp(keys[other_col], buf)) ++other_col;
          if(other_col >= keys.size()) {
            stringstream msg; msg << "basic_binary_col_adder could not find " << buf << " which is paired with " << keys[column];
            throw runtime_error(msg.str());
          }

          col_info_t i; i.index = numeric_limits<size_t>::max(); i.need_double = 0; i.need_c_str = 0; i.passthrough = 1;
          col_info_t* p = &((*cols.insert(typename map<size_t, col_info_t>::value_type(column, i)).first).second);
          if(typeid(op_in1_t) == typeid(double)) p->need_double = 1;
          else p->need_c_str = 1;
          if((*ii).remove_source) p->passthrough = 0;

          p = &((*cols.insert(typename map<size_t, col_info_t>::value_type(other_col, i)).first).second);
          if(typeid(op_in2_t) == typeid(double)) p->need_double = 1;
          else p->need_c_str = 1;
          if((*ii).remove_other) p->passthrough = 0;

          next = buf;
          generate_substitution(keys[column], (*ii).new_key.c_str(), ovector, rc, buf, next, end);
          vector<new_col_info_t>& nciv = new_cols[column];
          nciv.resize(nciv.size() + 1);
          nciv.back().other_col = other_col;
          nciv.back().new_key.assign(buf, next - buf - 1);
          nciv.back().op = &(*ii).op;
        }
      }
    }

    for(column = 0; column < keys.size(); ++column) {
      typename map<size_t, col_info_t>::iterator i = cols.find(column);
      if(i == cols.end() || (*i).second.passthrough)
        this->output_token(keys[column], lens[column]);
    }

    columns.reserve(cols.size());
    for(typename map<size_t, col_info_t>::iterator i = cols.begin(); i != cols.end(); ++i) {
      (*i).second.index = columns.size();
      columns.resize(columns.size() + 1);
      col_t& c = columns.back();
      c.col = (*i).first;
      c.need_double = (*i).second.need_double;
      c.need_c_str = (*i).second.need_c_str;
      c.passthrough = (*i).second.passthrough;
    }

    for(typename map<size_t, vector<new_col_info_t> >::iterator i = new_cols.begin(); i != new_cols.end(); ++i) {
      for(typename vector<new_col_info_t>::iterator j = (*i).second.begin(); j != (*i).second.end(); ++j) {
        new_columns.resize(new_columns.size() + 1);
        new_col_t& c = new_columns.back();
        c.col_index = cols[(*i).first].index;
        c.other_col_index = cols[(*j).other_col].index;
        c.op = *(*j).op;
        this->output_token((*j).new_key.c_str(), (*j).new_key.size());
      }
    }

    delete[] lens;
    delete[] buf;
    first_row = 0;
    keys.clear();
    for(vector<char*>::const_iterator i = key_storage.begin(); i != key_storage.end(); ++i) delete[] *i;
    key_storage.clear();
    key_storage_next = 0;
    key_storage_end = 0;
  }
  else {
    for(typename vector<new_col_t>::iterator i = new_columns.begin(); i != new_columns.end(); ++i) {
      op_in1_t& in1 = get_in_value((*i).col_index, (op_in1_t*)0);
      op_in2_t& in2 = get_in_value((*i).other_col_index, (op_in2_t*)0);
      op_out_t val = (*i).op(in1, in2);
      this->output_token(val);
    }
  }
  this->output_line();
  column = 0;
  ci = columns.begin();
}


}


#endif


