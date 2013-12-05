#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include "table.h"

#undef TABLE_DIMENSIONS_DEBUG_PRINTS

using namespace std;
using namespace std::tr1;


namespace table {

void resize_buffer(char*& buf, char*& next, char*& end, char** resize_end)
{
  char* old = buf;
  size_t size = next - buf;
  size_t cap = end - buf;
  size_t resize = resize_end ? end - *resize_end : 0;

  size_t new_cap = cap * 2;
  buf = new char[new_cap];
  memcpy(buf, old, size);
  delete[] old;

  next = buf + size;
  end = buf + new_cap;
  if(resize_end) *resize_end = end - resize;
}


void generate_substitution(const char* token, const char* replace_with, const int* ovector, int num_captured, char*& buf, char*& next, char*& end)
{
  for(const char* rp = replace_with; 1; ++rp) {
    int backref = numeric_limits<int>::max();
    if(*rp == '\\' && isdigit(*(rp + 1))) backref = *(rp + 1) - '0';
    if(backref < num_captured) {
      const char* cs = token + ovector[backref * 2];
      const char* ce = token + ovector[backref * 2 + 1];
      while(cs < ce) {
        if(next >= end) resize_buffer(buf, next, end);
        *next++ = *cs++;
      }
      ++rp;
    }
    else {
      if(next >= end) resize_buffer(buf, next, end);
      *next++ = *rp;
      if(!*rp) break;
    }
  }
}

static const double pow10[] = {1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0, 1000000.0, 10000000.0, 100000000.0, 1000000000.0};

static void strreverse(char* begin, char* end)
{
    char aux;
    while (end > begin) aux = *end, *end-- = *begin, *begin++ = aux;
}

int dtostr(double value, char* str, int prec)
{
  if(isnan(value)) { str[0] = '\0'; return 0; }
  if(value > (double)(0x7FFFFFFF)) { return sprintf(str, "%.6g", value); }

  // precision of >= 10 can lead to overflow errors
  if (prec < 0) { prec = 0; }
  else if (prec > 9) { prec = 9; }

  bool neg = 0;
  if(value < 0) { neg = 1; value = -value; }
  int whole = (int)value;
  double tmp = (value - whole) * pow10[prec];
  uint32_t frac = (uint32_t)(tmp);
  double diff = tmp - frac;

  if(diff > 0.5) { ++frac; if(frac >= pow10[prec]) { frac = 0; ++whole; } } // handle rollover, e.g.  case 0.99 with prec 1 is 1.0
  else if(diff == 0.5 && ((frac == 0) || (frac & 1))) { ++frac; } // if halfway, round up if odd, OR if last digit is 0.  That last part is strange

  char* wstr = str;
  if(prec == 0) {
    diff = value - whole;
    if(diff > 0.5) { ++whole; } // greater than 0.5, round up, e.g. 1.6 -> 2
    else if(diff == 0.5 && (whole & 1)) { ++whole; } // exactly 0.5 and ODD, then round up; 1.5 -> 2, but 2.5 -> 2
  }
  else if(frac) {
    int count = prec;
    while(!(frac % 10)) { --count; frac /= 10; }
    do { --count; *wstr++ = '0' + char(frac % 10); } while(frac /= 10);
    while(count-- > 0) *wstr++ = '0';
    *wstr++ = '.';
  }

  do { *wstr++ = '0' + char(whole % 10); } while(whole /= 10);
  if(neg) *wstr++ = '-';
  *wstr = '\0';
  strreverse(str, wstr-1);

  return wstr - str;
}


////////////////////////////////////////////////////////////////////////////////////////////////
// cstring_queue
////////////////////////////////////////////////////////////////////////////////////////////////

vector<cstring_queue::block_t> cstring_queue::free_blocks;

vector<cstring_queue::block_t>::iterator cstring_queue::add_block(size_t len)
{
  vector<block_t>::iterator fbi = free_blocks.begin();
  while(fbi != free_blocks.end() && (*fbi).data->cap <= len) ++fbi;

  vector<block_t>::iterator i;
  if(fbi != free_blocks.end()) {
    if(!data->blocks.size()) data->next_read = (*fbi).data->buf;
    i = data->blocks.insert(data->blocks.end(), *fbi);
    free_blocks.erase(fbi);
    //cout << "found_block " << len << ' ' << (*i).data->cap << endl;
  }
  else {
    size_t cap = default_block_cap;
    if(len >= cap) cap = len + 1;
    block_t b(cap);
    if(!data->blocks.size()) data->next_read = b.data->buf;
    i = data->blocks.insert(data->blocks.end(), b);
    //cout << "new_block " << len << ' ' << (*i).data->cap << endl;
  }

  (*i).data->next_write = (*i).data->buf;

  return i;
}

void cstring_queue::copy()
{
  if(data->rc <= 1) { cow = 0; return; }

  cstring_queue_data_t* old = data;
  data = new cstring_queue_data_t;
  data->rc = 1;
  for(vector<block_t>::const_iterator i = old->blocks.begin(); i != old->blocks.end(); ++i) {
    block_t t((*i).data->cap);
    memcpy(t.data->buf, (*i).data->buf, (*i).data->next_write - (*i).data->buf);
    t.data->last_token = (*i).data->last_token;
    t.data->next_write = (*i).data->next_write;
    data->blocks.push_back(t);
  }
  data->next_read = old->next_read;
  cow = 0;
}

void cstring_queue::clear()
{
  if(data->rc <= 1) {
    for(vector<block_t>::iterator i = data->blocks.begin(); i != data->blocks.end(); ++i)
      free_blocks.push_back(*i);
    data->blocks.clear();
    data->next_read = 0;
  }
  else {
    cstring_queue_data_t* old = data;
    --old->rc;
    data = new cstring_queue_data_t;
    data->rc = 1;
    data->next_read = 0;
  }
  cow = 0;
}


////////////////////////////////////////////////////////////////////////////////////////////////
// pass
////////////////////////////////////////////////////////////////////////////////////////////////

pass::~pass() {}

void pass::process_token(double token)
{
  char buf[32];
  dtostr(token, buf);
  process_token(buf);
}


////////////////////////////////////////////////////////////////////////////////////////////////
// threader
////////////////////////////////////////////////////////////////////////////////////////////////

void* threader_main(void* data)
{
  threader* t = static_cast<threader*>(data);

  bool done = 0;
  bool inc = 0;
  while(!done) {
    pthread_mutex_lock(&t->mutex);
    if(inc) {
      t->read_chunk = (t->read_chunk + 1) % 8;
      pthread_cond_signal(&t->prod_cond);
    }
    else inc = 1;
    while(t->read_chunk == t->write_chunk) {
      pthread_cond_wait(&t->cons_cond, &t->mutex);
    }
    pthread_mutex_unlock(&t->mutex);

    for(char* cur = t->chunks[t->read_chunk].start; 1; ++cur) {
      if(*cur == '\x01') { t->out->process_token(*reinterpret_cast<double*>(++cur)); cur += sizeof(double) - 1; }
      else if(*cur == '\x02') { t->out->process_line(); }
      else if(*cur == '\x03') { break; }
      else if(*cur == '\x04') { done = 1; break; }
      else { size_t len = strlen(cur); t->out->process_token(cur); cur += len; }
    }
  }

  t->out->process_stream();

  return 0;
}

void threader::resize_write_chunk(size_t min_size)
{
  delete[] chunks[write_chunk].start;
  chunks[write_chunk].start = new char[min_size * 2];
  write_chunk_next = chunks[write_chunk].start;
  chunks[write_chunk].end = chunks[write_chunk].start + min_size * 2;
}

void threader::inc_write_chunk(bool term)
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

threader::threader() : thread_created(0) { init(); }
threader::threader(pass& out) : thread_created(0) { init(out); }

threader::~threader()
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

threader& threader::init() {
  out = 0;
  for(int c = 0; c < 8; ++c) {
    delete[] chunks[c].start;
    chunks[c].start = new char[8 * 1024];
    chunks[c].end = chunks[c].start + 8 * 1024;
  }
  write_chunk = 0;
  write_chunk_next = chunks[0].start;
  read_chunk = 0;
  return *this;
}

threader& threader::init(pass& out) { init(); return set_out(out); }
threader& threader::set_out(pass& out) { this->out = &out; return *this; }

void threader::process_token(const char* token)
{
  if(!thread_created) {
    if(!out) throw runtime_error("threader has no out");
    pthread_mutex_init(&mutex, 0);
    pthread_cond_init(&prod_cond, 0);
    pthread_cond_init(&cons_cond, 0);
    pthread_create(&thread, 0, threader_main, this);
    thread_created = 1;
  }

  size_t len = strlen(token);
  if(size_t(chunks[write_chunk].end - write_chunk_next) < len + 2) {
    if(write_chunk_next == chunks[write_chunk].start) resize_write_chunk(len + 2);
    else inc_write_chunk();
  }
  memcpy(write_chunk_next, token, len + 1);
  write_chunk_next += len + 1;
}

void threader::process_token(double token)
{
  if(!thread_created) {
    if(!out) throw runtime_error("threader has no out");
    pthread_mutex_init(&mutex, 0);
    pthread_cond_init(&prod_cond, 0);
    pthread_cond_init(&cons_cond, 0);
    pthread_create(&thread, 0, threader_main, this);
    thread_created = 1;
  }

  if(size_t(chunks[write_chunk].end - write_chunk_next) < size_t(sizeof(double) + 2)) inc_write_chunk();
  *write_chunk_next++ = '\x01';
  memcpy(write_chunk_next, &token, sizeof(double));
  write_chunk_next += sizeof(double);
}

void threader::process_line()
{
  if(!thread_created) {
    if(!out) throw runtime_error("threader has no out");
    out->process_line();
    return;
  }

  if(chunks[write_chunk].end - write_chunk_next < 2) inc_write_chunk();
  *write_chunk_next++ = '\x02';
}

void threader::process_stream()
{
  if(!thread_created) {
    if(!out) throw runtime_error("threader has no out");
    out->process_stream();
    return;
  }

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

subset_tee_dest_data_t::~subset_tee_dest_data_t()
{
  for(vector<pcre*>::iterator i = regex.begin(); i != regex.end(); ++i) pcre_free(*i);
  for(vector<pcre*>::iterator i = regex_except.begin(); i != regex_except.end(); ++i) pcre_free(*i);
}

subset_tee::subset_tee() { init(); }
subset_tee::subset_tee(pass& dest) { init(dest); }

subset_tee& subset_tee::init() {
  dest_data.clear();
  ddi = dest_data.end();
  first_row = 1;
  column = 0;
  this->dest.clear();
  return *this;
}

subset_tee& subset_tee::init(pass& dest) { init(); return set_dest(dest); }
subset_tee& subset_tee::set_dest(pass& dest) { ddi = dest_data.insert(map<pass*, subset_tee_dest_data_t>::value_type(&dest, subset_tee_dest_data_t())).first; return *this; }

subset_tee& subset_tee::add_data(bool regex, const char* key)
{
  if(ddi == dest_data.end()) throw runtime_error("no current destination");

  if(regex) {
    const char* err; int err_off; pcre* p = pcre_compile(key, 0, &err, &err_off, 0);
    if(!p) throw runtime_error("subset_tee can't compile data regex");
    (*ddi).second.regex.push_back(p);
  }
  else (*ddi).second.key.insert(key);

  return *this;
}

subset_tee& subset_tee::add_exception(bool regex, const char* key)
{
  if(ddi == dest_data.end()) throw runtime_error("no current destination");

  if(regex) {
    const char* err; int err_off; pcre* p = pcre_compile(key, 0, &err, &err_off, 0);
    if(!p) throw runtime_error("subset_tee can't compile exception regex");
    (*ddi).second.regex_except.push_back(p);
  }
  else (*ddi).second.key_except.insert(key);

  return *this;
}

void subset_tee::process_token(const char* token)
{
  if(first_row) {
    for(std::map<pass*, subset_tee_dest_data_t>::iterator di = dest_data.begin(); di != dest_data.end(); ++di) {
      subset_tee_dest_data_t& d = (*di).second;

      size_t len = strlen(token);
      bool add = 0;
      set<string>::const_iterator ki = d.key.find(token);
      if(ki != d.key.end()) add = 1;
      else {
        for(vector<pcre*>::const_iterator ri = d.regex.begin(); ri != d.regex.end(); ++ri) {
          int ovector[30]; int rc = pcre_exec(*ri, 0, token, len, 0, 0, ovector, 30);
          if(rc >= 0) { add = 1; break; }
          else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("subset_tee match error");
        }
      }

      set<string>::const_iterator kei = d.key_except.find(token);
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
        (*di).first->process_token(token);
      }
    }
  }
  else {
    for(; di != dest.end() && (*di).first == column; ++di)
      (*di).second->process_token(token);
  }

  ++column;
}

void subset_tee::process_token(double token)
{
  if(first_row) { char buf[32]; dtostr(token, buf); process_token(buf); return; }

  for(; di != dest.end() && (*di).first == column; ++di)
    (*di).second->process_token(token);

  ++column;
}

void subset_tee::process_line()
{
  first_row = 0;
  for(std::map<pass*, subset_tee_dest_data_t>::iterator ddi = dest_data.begin(); ddi != dest_data.end(); ++ddi)
    if((*ddi).second.has_data)
      (*ddi).first->process_line();
  column = 0;
  di = dest.begin();
}

void subset_tee::process_stream()
{
  for(std::map<pass*, subset_tee_dest_data_t>::iterator ddi = dest_data.begin(); ddi != dest_data.end(); ++ddi)
    (*ddi).first->process_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// ordered_tee
////////////////////////////////////////////////////////////////////////////////////////////////

ordered_tee::ordered_tee() { init(); }
ordered_tee::ordered_tee(pass& out1, pass& out2) { init(out1, out2); }

ordered_tee::~ordered_tee()
{
  for(vector<char*>::iterator i = data.begin(); i != data.end(); ++i) delete[] *i;
}

ordered_tee& ordered_tee::init()
{
  out.clear();
  first_row = 1;
  num_columns = 0;
  for(vector<char*>::iterator i = data.begin(); i != data.end(); ++i) delete[] *i;
  data.clear();
  next = 0;
  end = 0;
  return *this;
}

ordered_tee& ordered_tee::init(pass& out1, pass& out2) { init(); out.push_back(&out1); out.push_back(&out2); return *this; }
ordered_tee& ordered_tee::add_out(pass& out) { this->out.push_back(&out); return *this; }

void ordered_tee::process_token(const char* token)
{
  if(!out.size()) throw runtime_error("ordered_tee::process_token no outs");
  out[0]->process_token(token);

  if(first_row) ++num_columns;

  size_t len = strlen(token);
  if(!next || (len + 2) > size_t(end - next)) {
    if(next) *next++ = '\x03';
    size_t cap = 256 * 1024;
    if(cap < len + 2) cap = len + 2;
    data.push_back(new char[cap]);
    next = data.back();
    end = data.back() + cap;
  } 
  memcpy(next, token, len + 1);
  next += len + 1;
}

void ordered_tee::process_token(double token)
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

void ordered_tee::process_line()
{
  if(!out.size()) throw runtime_error("ordered_tee::process_line no outs");

  out[0]->process_line();
  first_row = 0;
}

void ordered_tee::process_stream()
{
  if(next) *next++ = '\x03';

  vector<pass*>::iterator oi = out.begin();
  (*oi)->process_stream();
  for(++oi; oi != out.end(); ++oi) {
    vector<pass*>::iterator noi = oi; ++noi;
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
          (*oi)->process_token(p);
          while(*p) ++p;
          ++p;
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

stacker::stacker(stack_action_e default_action) { init(default_action); }
stacker::stacker(pass& out, stack_action_e default_action) { init(out, default_action); }
stacker::~stacker() {}

stacker& stacker::re_init()
{
  first_line = 1;
  stack_keys.clear();
  actions.clear();
  last_leave = 0;

  column = 0;
  stack_column = 0;
  leave_tokens.clear();
  stack_tokens.clear();
  return *this;
}

stacker& stacker::init(stack_action_e default_action)
{
  this->out = 0;
  this->default_action = default_action;
  keyword_actions.clear();
  regex_actions.clear();
  re_init();
  return *this;
}

stacker& stacker::init(pass& out, stack_action_e default_action) { init(default_action); return set_out(out); }
stacker& stacker::set_out(pass& out) { this->out = &out; return *this; }

stacker& stacker::add_action(bool regex, const char* key, stack_action_e action)
{
  if(regex) {
    const char* err; int err_off; pcre* p = pcre_compile(key, 0, &err, &err_off, 0);
    if(!p) throw runtime_error("stacker can't compile group regex");
    regex_actions.resize(regex_actions.size() + 1);
    regex_actions.back().regex = p;
    regex_actions.back().action = action;
  }
  else keyword_actions[key] = action;

  return *this;
}

void stacker::process_token(const char* token)
{
  if(first_line) {
    if(!out) throw runtime_error("stacker has no out");
    stack_action_e action = default_action;
    map<string, stack_action_e>::iterator i = keyword_actions.find(token);
    if(i != keyword_actions.end()) action = (*i).second;
    else {
      size_t len = strlen(token);
      for(vector<regex_stack_action_t>::iterator j = regex_actions.begin(); j != regex_actions.end(); ++j) {
        int ovector[30]; int rc = pcre_exec((*j).regex, 0, token, len, 0, 0, ovector, 30);
        if(rc >= 0) { action = (*j).action; break; }
        else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("stacker match error");
      }
    }
    actions.push_back(action);
    if(action == ST_LEAVE) { leave_tokens.push(token); last_leave = column; out->process_token(token); }
    else if(action == ST_STACK) stack_keys.push_back(token);
  }
  else {
    if(column >= actions.size()) throw runtime_error("too many columns");
    else if(actions[column] == ST_LEAVE) {
      leave_tokens.push(token);
      if(column == last_leave) {
        for(stack_column = 0; !stack_tokens.empty(); ++stack_column) {
          for(cstring_queue::const_iterator i = leave_tokens.begin(); i != leave_tokens.end(); ++i) { out->process_token(*i); }
          out->process_token(stack_keys[stack_column].c_str());
          out->process_token(stack_tokens.front());
          stack_tokens.pop();
          out->process_line();
        }
      }
    }
    else if(actions[column] == ST_STACK) { 
      if(column < last_leave) stack_tokens.push(token);
      else {
        for(cstring_queue::const_iterator i = leave_tokens.begin(); i != leave_tokens.end(); ++i) out->process_token(*i);
        out->process_token(stack_keys[stack_column++].c_str());
        out->process_token(token);
        out->process_line();
      }
    }
  }

  ++column;
}

void stacker::process_line()
{
  if(first_line) {
    if(!out) throw runtime_error("stacker has no out");
    out->process_token("keyword");
    out->process_token("data");
    out->process_line();
    first_line = 0;
  }
  column = 0;
  stack_column = 0;
  leave_tokens.clear();
}

void stacker::process_stream()
{
  if(!out) throw runtime_error("stacker has no out");
  out->process_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// splitter
////////////////////////////////////////////////////////////////////////////////////////////////

splitter::splitter(split_action_e default_action) : group_tokens(0), split_by_tokens(0), split_tokens(0) { init(default_action); }
splitter::splitter(pass& out, split_action_e default_action) : group_tokens(0), split_by_tokens(0), split_tokens(0)  { init(out, default_action); }

splitter& splitter::init(split_action_e default_action)
{
  this->out = 0;
  this->default_action = default_action;
  keyword_actions.clear();
  regex_actions.clear();

  first_line = 1;
  actions.clear();
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

  for(map<char*, size_t, cstr_less>::const_iterator i = out_split_keys.begin(); i != out_split_keys.end(); ++i)
    delete[] (*i).first;
  for(vector<char*>::const_iterator i = group_storage.begin(); i != group_storage.end(); ++i)
    delete[] *i;
  group_storage_next = 0;
  group_storage_end = 0;
  out_split_keys.clear();
  data.clear();

  return *this;
}

splitter& splitter::init(pass& out, split_action_e default_action) { init(default_action); return set_out(out); }

splitter::~splitter()
{
  delete[] group_tokens;
  delete[] split_by_tokens;
  delete[] split_tokens;
  for(map<char*, size_t, cstr_less>::const_iterator i = out_split_keys.begin(); i != out_split_keys.end(); ++i)
    delete[] (*i).first;
  for(vector<char*>::const_iterator i = group_storage.begin(); i != group_storage.end(); ++i)
    delete[] *i;
}

splitter& splitter::set_out(pass& out) { this->out = &out; return *this; }

splitter& splitter::add_action(bool regex, const char* key, split_action_e action)
{
  if(regex) {
    const char* err; int err_off; pcre* p = pcre_compile(key, 0, &err, &err_off, 0);
    if(!p) throw runtime_error("splitter can't compile data regex");
    regex_actions.resize(regex_actions.size() + 1);
    regex_actions.back().regex = p;
    regex_actions.back().action = action;
  }
  else keyword_actions[key] = action;

  return *this;
}

void splitter::process_token(const char* token)
{
  if(first_line) {
    if(!out) throw runtime_error("splitter has no out");
    split_action_e action = default_action;
    map<string, split_action_e>::iterator i = keyword_actions.find(token);
    if(i != keyword_actions.end()) action = (*i).second;
    else {
      size_t len = strlen(token);
      for(vector<regex_split_action_t>::iterator j = regex_actions.begin(); j != regex_actions.end(); ++j) {
        int ovector[30]; int rc = pcre_exec((*j).regex, 0, token, len, 0, 0, ovector, 30);
        if(rc >= 0) { action = (*j).action; break; }
        else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("splitter match error");
      }
    }
    actions.push_back(action);
    if(action == SP_GROUP) group_keys.push_back(token);
    else if(action == SP_SPLIT) split_keys.push_back(token);
  }
  else {
    if(actions[column] == SP_GROUP) {
      for(const char* p = token; 1; ++p) {
        if(group_tokens_next >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end);
        *group_tokens_next++ = *p;
        if(!*p) break;
      }
    }
    else if(actions[column] == SP_SPLIT_BY) {
      for(const char* p = token; 1; ++p) {
        if(split_by_tokens_next >= split_by_tokens_end) resize_buffer(split_by_tokens, split_by_tokens_next, split_by_tokens_end);
        *split_by_tokens_next++ = *p;
        if(!*p) break;
      }
    }
    else if(actions[column] == SP_SPLIT) {
      for(const char* p = token; 1; ++p) {
        if(split_tokens_next >= split_tokens_end) resize_buffer(split_tokens, split_tokens_next, split_tokens_end);
        *split_tokens_next++ = *p;
        if(!*p) break;
      }
    }
  }

  ++column;
}

void splitter::process_line()
{
  if(first_line) {
    if(!out) throw runtime_error("splitter has no out");
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
      *group_tokens_next++ = ' ';
      size_t sk_len = group_tokens_next - group_tokens;
      for(char* sbtp = split_by_tokens; *sbtp != '\x03'; ++sbtp) {
        group_tokens_next = group_tokens + sk_len;
        for(; 1; ++sbtp) {
          if(group_tokens_next >= group_tokens_end) resize_buffer(group_tokens, group_tokens_next, group_tokens_end);
          *group_tokens_next++ = *sbtp;
          if(!*sbtp) break;
        }
      }

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

void splitter::process_stream()
{
  if(!out) throw runtime_error("splitter has no out");

  for(vector<string>::const_iterator i = group_keys.begin(); i != group_keys.end(); ++i)
    out->process_token((*i).c_str());
  {
    vector<char*> osk(out_split_keys.size());
    for(map<char*, size_t, cstr_less>::const_iterator i = out_split_keys.begin(); i != out_split_keys.end(); ++i)
      osk[(*i).second] = (*i).first;
    for(vector<char*>::const_iterator i = osk.begin(); i != osk.end(); ++i)
      out->process_token(*i);
  }
  out->process_line();

  map<char*, size_t, cstr_less>::size_type num_out_split_keys = out_split_keys.size();
  for(map<char*, size_t, cstr_less>::const_iterator i = out_split_keys.begin(); i != out_split_keys.end(); ++i)
    delete[] (*i).first;
  out_split_keys.clear();

  for(data_t::const_iterator i = data.begin(); i != data.end(); ++i) {
    for(char* p = (*i).first; *p != '\x03'; ++p) {
      out->process_token(p);
      while(*p) ++p;
    }
    size_t tokens = 0;
    for(vector<string>::const_iterator j = (*i).second.begin(); j != (*i).second.end(); ++j, ++tokens)
      out->process_token((*j).c_str());
    while(tokens++ < num_out_split_keys)
      out->process_token("");
    out->process_line();
  }

  for(vector<char*>::const_iterator i = group_storage.begin(); i != group_storage.end(); ++i)
    delete[] *i;
  group_storage.clear();
  data.clear();

  out->process_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// row_joiner
////////////////////////////////////////////////////////////////////////////////////////////////

row_joiner::row_joiner() : out(0) {}
row_joiner::row_joiner(pass& out, const char* table_name) { init(out, table_name); }

row_joiner& row_joiner::init(pass& out, const char* table_name)
{
  this->out = &out;
  this->table_name.clear();
  table = 0;
  first_line = 1;
  more_lines = 0;
  column = 0;
  num_columns.clear();
  keys.clear();
  data.clear();
  add_table_name(table_name);
  return *this;
}

row_joiner& row_joiner::add_table_name(const char* name)
{
  if(name) table_name.push_back(name);
  else {
    stringstream ss; ss << "Table " << table_name.size();
    table_name.push_back(ss.str());
  }
  num_columns.push_back(0);

  return *this;
}

void row_joiner::process_token(const char* token)
{
  if(!first_line) data[table].push(token);
  else {
    if(!more_lines) {
      if(table >= table_name.size()) add_table_name();
      ++num_columns[table];
      keys.push_back(token);
    }
    else {
      size_t c = column;
      for(size_t t = 0; t < table; ++t)
        c += num_columns[t];
      if(keys[c].compare(token) && keys[c].compare(string(token) + " of " + table_name[table]))
        throw runtime_error("column keys don't match previous tables");
      ++column;
    }
  }
}

void row_joiner::process_line()
{
  if(first_line) {
    first_line = 0;
    data.resize(data.size() + 1);
    if(more_lines) {
      if(column != num_columns[table]) throw runtime_error("num_columns doesn't match");
      column = 0;
    }
  }
}

void row_joiner::process_stream()
{
  ++table;
  first_line = 1;
}

void row_joiner::process_lines()
{
  if(!out) throw runtime_error("row_joiner has no out");

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
      out->process_token((*ki).c_str());
    }
    out->process_line();
  }

  vector<size_t>::const_iterator nci = num_columns.begin();
  vector<cstring_queue>::iterator di = data.begin();
  while(!(*di).empty()) {
    for(size_t col = 0; col < (*nci) && !(*di).empty(); ++col) {
      out->process_token((*di).front());
      (*di).pop();
    }

    //next table
    ++di;
    ++nci;
    if(di == data.end()) {
      out->process_line();
      di = data.begin();
      nci = num_columns.begin();
    }
  }

  table = 0;
  first_line = 1;
  more_lines = 1;
  data.clear();
}

void row_joiner::process()
{
  if(!out) throw runtime_error("row_joiner has no out");

  if(data.size()) process_lines();

  out->process_stream();
  table_name.clear();
  num_columns.clear();
  keys.clear();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// substitutor
////////////////////////////////////////////////////////////////////////////////////////////////

substitutor::substitutor() : buf(0) { init(); }
substitutor::substitutor(pass& out) : buf(0) { init(out); }

substitutor::~substitutor()
{
  for(vector<pcre*>::iterator i = exceptions.begin(); i != exceptions.end(); ++i) pcre_free(*i);
  delete[] buf;
}

substitutor& substitutor::init()
{
  out = 0;
  subs.clear();
  for(vector<pcre*>::iterator i = exceptions.begin(); i != exceptions.end(); ++i) pcre_free(*i);
  exceptions.clear();
  column_subs.clear();
  first_line = 1;
  column = 0;
  delete[] buf;
  buf = new char[2048];
  end = buf + 2048;
  return *this;
}

substitutor& substitutor::init(pass& out) { init(); return set_out(out); }
substitutor& substitutor::set_out(pass& out) { this->out = &out; return *this; }

substitutor& substitutor::add(const char* regex, const char* from, const char* to)
{
  subs.resize(subs.size() + 1);

  const char* err; int err_off;
  pcre* pr = pcre_compile(regex, 0, &err, &err_off, 0);
  if(!pr) throw runtime_error("substitutor can't compile regex");
  subs.back().regex = pr;

  pcre* pf = pcre_compile(from, 0, &err, &err_off, 0);
  if(!pf) throw runtime_error("substitutor can't compile from regex");
  subs.back().from = pf;

  pcre_extra* pfe = pcre_study(pf, PCRE_STUDY_JIT_COMPILE, &err);
  if(!pf) throw runtime_error("substitutor can't study from");
  subs.back().from_extra = pfe;

  subs.back().to = to;

  return *this;
}

substitutor& substitutor::add_exception(const char* key)
{
  const char* err; int err_off; pcre* p = pcre_compile(key, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("substitutor can't compile exception regex");
  exceptions.push_back(p);
  return *this;
}

void substitutor::process_token(const char* token)
{
  if(first_line) {
    sub_t* s = 0;
    size_t len = strlen(token);
    for(vector<sub_t>::iterator si = subs.begin(); si != subs.end(); ++si) {
      int ovector[30]; int rc = pcre_exec((*si).regex, 0, token, len, 0, 0, ovector, 30);
      if(rc >= 0) { s = &(*si); break; }
      else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("substitutor column match error");
    }
    if(s) {
      for(vector<pcre*>::const_iterator ei = exceptions.begin(); ei != exceptions.end(); ++ei) {
        int ovector[30]; int rc = pcre_exec(*ei, 0, token, len, 0, 0, ovector, 30);
        if(rc >= 0) { s = 0; break; }
        else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("substitutor exception match error");
      }
    }
    column_subs.push_back(s);
    out->process_token(token);
  }
  else {
    sub_t* s = column_subs[column];
    if(!s) out->process_token(token);
    else {
      size_t len = strlen(token);
      int ovector[30]; int rc = pcre_exec(s->from, s->from_extra, token, len, 0, 0, ovector, 30);
      if(rc < 0) {
        if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("substitutor exception match error");
        out->process_token(token);
      }
      else {
        char* next = buf;
        if(!rc) rc = 10;
        generate_substitution(token, s->to.c_str(), ovector, rc, buf, next, end);
        out->process_token(buf);
      }
    }
  }

  ++column;
}

void substitutor::process_token(double token)
{
  if(first_line) { char buf[32]; dtostr(token, buf); process_token(buf); return; }

  sub_t* s = column_subs[column];
  if(!s) out->process_token(token);
  else {
    char sbuf[32];
    int len = dtostr(token, sbuf);
    int ovector[30]; int rc = pcre_exec(s->from, s->from_extra, sbuf, len, 0, 0, ovector, 30);
    if(rc < 0) {
      if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("substitutor exception match error");
      out->process_token(token);
    }
    else {
      char* next = buf;
      if(!rc) rc = 10;
      generate_substitution(sbuf, s->to.c_str(), ovector, rc, buf, next, end);
      out->process_token(buf);
    }
  }

  ++column;
}

void substitutor::process_line()
{
  if(first_line) {
    if(!out) throw runtime_error("substitutor has no out");
    first_line = 0;
  }

  out->process_line();
  column = 0;
}

void substitutor::process_stream()
{
  if(!out) throw runtime_error("substitutor has no out");
  delete[] buf;
  buf = 0;
  out->process_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// col_adder
////////////////////////////////////////////////////////////////////////////////////////////////

col_adder::col_adder() : buf(0) { init(); }
col_adder::col_adder(pass& out) : buf(0) { init(out); }

col_adder::~col_adder()
{
  for(vector<pcre*>::iterator i = exceptions.begin(); i != exceptions.end(); ++i) pcre_free(*i);
  delete[] buf;
}

col_adder& col_adder::init()
{
  out = 0;
  subs.clear();
  for(vector<pcre*>::iterator i = exceptions.begin(); i != exceptions.end(); ++i) pcre_free(*i);
  exceptions.clear();
  column_subs.clear();
  first_line = 1;
  column = 0;
  delete[] buf;
  buf = new char[2048];
  end = buf + 2048;
  return *this;
}

col_adder& col_adder::init(pass& out) { init(); return set_out(out); }
col_adder& col_adder::set_out(pass& out) { this->out = &out; return *this; }

col_adder& col_adder::add(const char* regex, const char* new_key, const char* from, const char* to)
{
  subs.resize(subs.size() + 1);

  const char* err; int err_off;
  pcre* pr = pcre_compile(regex, 0, &err, &err_off, 0);
  if(!pr) throw runtime_error("col_adder can't compile regex");
  subs.back().regex = pr;

  subs.back().new_key = new_key;

  pcre* pf = pcre_compile(from, 0, &err, &err_off, 0);
  if(!pf) throw runtime_error("col_adder can't compile from regex");
  subs.back().from = pf;

  pcre_extra* pfe = pcre_study(pf, PCRE_STUDY_JIT_COMPILE, &err);
  if(!pf) throw runtime_error("col_adder can't study from");
  subs.back().from_extra = pfe;

  subs.back().to = to;

  return *this;
}

col_adder& col_adder::add_exception(const char* key)
{
  const char* err; int err_off; pcre* p = pcre_compile(key, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("col_adder can't compile exception regex");
  exceptions.push_back(p);
  return *this;
}

void col_adder::process_token(const char* token)
{
  if(first_line) {
    sub_t* s = 0;
    size_t len = strlen(token);
    int ovector[30];
    int rc = 0;
    for(vector<sub_t>::iterator si = subs.begin(); si != subs.end(); ++si) {
      rc = pcre_exec((*si).regex, 0, token, len, 0, 0, ovector, 30);
      if(rc >= 0) { s = &(*si); break; }
      else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("col_adder column match error");
    }
    if(s) {
      for(vector<pcre*>::const_iterator ei = exceptions.begin(); ei != exceptions.end(); ++ei) {
        int ovector[30]; int rc = pcre_exec(*ei, 0, token, len, 0, 0, ovector, 30);
        if(rc >= 0) { s = 0; break; }
        else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("col_adder exception match error");
      }
    }
    column_subs.push_back(s);
    out->process_token(token);
    if(s) {
      char* next = buf;
      if(!rc) rc = 10;
      generate_substitution(token, s->new_key.c_str(), ovector, rc, buf, next, end);
      out->process_token(buf);
    }
  }
  else {
    sub_t* s = column_subs[column];
    out->process_token(token);
    if(s) {
      size_t len = strlen(token);
      int ovector[30]; int rc = pcre_exec(s->from, s->from_extra, token, len, 0, 0, ovector, 30);
      if(rc < 0) {
        if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("col_adder exception match error");
        out->process_token(token);
      }
      else {
        char* next = buf;
        if(!rc) rc = 10;
        generate_substitution(token, s->to.c_str(), ovector, rc, buf, next, end);
        out->process_token(buf);
      }
    }
  }

  ++column;
}

void col_adder::process_token(double token)
{
  if(first_line) { char buf[32]; dtostr(token, buf); process_token(buf); return; }

  sub_t* s = column_subs[column];
  out->process_token(token);
  if(s) {
    char sbuf[32];
    size_t len = dtostr(token, sbuf);
    int ovector[30]; int rc = pcre_exec(s->from, s->from_extra, sbuf, len, 0, 0, ovector, 30);
    if(rc < 0) {
      if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("col_adder exception match error");
      out->process_token(token);
    }
    else {
      char* next = buf;
      if(!rc) rc = 10;
      generate_substitution(sbuf, s->to.c_str(), ovector, rc, buf, next, end);
      out->process_token(buf);
    }
  }

  ++column;
}

void col_adder::process_line()
{
  if(first_line) {
    if(!out) throw runtime_error("col_adder has no out");
    first_line = 0;
  }

  out->process_line();
  column = 0;
}

void col_adder::process_stream()
{
  if(!out) throw runtime_error("col_adder has no out");
  delete[] buf;
  buf = 0;
  out->process_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// col_pruner
////////////////////////////////////////////////////////////////////////////////////////////////

col_pruner::col_pruner() : has_data(0) { init(); }
col_pruner::col_pruner(pass& out) : has_data(0) { init(out); }

col_pruner::~col_pruner()
{
  delete[] has_data;
  for(vector<char*>::iterator i = data.begin(); i != data.end(); ++i) delete[] *i;
}

col_pruner& col_pruner::init()
{
  this->out = 0;
  first_row = 1;
  passthrough = 0;
  column = 0;
  delete[] has_data; has_data = 0;
  for(vector<char*>::iterator i = data.begin(); i != data.end(); ++i) delete[] *i;
  data.clear();
  next = 0;
  return *this;
}

col_pruner& col_pruner::init(pass& out) { init(); return set_out(out); }
col_pruner& col_pruner::set_out(pass& out) { this->out = &out; return *this; }

void col_pruner::process_token(const char* token)
{
  if(passthrough) { out->process_token(token); return; }

  if(first_row) {
    if(!out) throw runtime_error("col_pruner has no out");
  }
  else if(token[0]) {
    if(!(has_data[column / 32] & (1 << (column % 32)))) {
      has_data[column / 32] |= 1 << (column % 32);
      ++columns_with_data;
    }
  }

  size_t len = strlen(token);
  if(!next || (len + 2) > size_t(end - next)) {
    if(next) *next++ = '\x03';
    size_t cap = 256 * 1024;
    if(cap < len + 2) cap = len + 2;
    data.push_back(new char[cap]);
    next = data.back();
    end = data.back() + cap;
  } 
  memcpy(next, token, len + 1);
  next += len + 1;
  ++column;
}

void col_pruner::process_token(double token)
{
  if(passthrough) { out->process_token(token); return; }

  if(first_row) {
    if(!out) throw runtime_error("col_pruner has no out");
  }
  else if(!isnan(token)) {
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

void col_pruner::process_line()
{
  if(passthrough) { out->process_line(); return; }

  if(first_row) {
    if(!out) throw runtime_error("col_pruner has no out");
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
          out->process_token(*reinterpret_cast<const double*>(++p));
          p += sizeof(double);
        }
        else {
          out->process_token(p);
          while(*p) ++p;
          ++p;
        }
        if(++c >= num_columns) {
          out->process_line();
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

void col_pruner::process_stream()
{
  if(passthrough) { out->process_stream(); return; }

  if(!out) throw runtime_error("col_pruner has no out");

  if(next) *next++ = '\x03';

  size_t c = 0;
  for(vector<char*>::iterator i = data.begin(); i != data.end(); ++i) {
    const char* p = *i;
    while(*p != '\03') {
      if(has_data[c / 32] & (1 << (c % 32))) {
        if(*p == '\x01') {
          out->process_token(*reinterpret_cast<const double*>(++p));
          p += sizeof(double);
        }
        else {
          out->process_token(p);
          while(*p) ++p;
          ++p;
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
        out->process_line();
        c = 0;
      }
    }
    delete[] *i;
  }
  delete[] has_data; has_data = 0;
  data.clear();
  next = 0;
  out->process_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// combiner
////////////////////////////////////////////////////////////////////////////////////////////////

combiner::combiner() { init(); }
combiner::combiner(pass& out) { init(out); }

combiner::~combiner()
{
  for(vector<pair<pcre*, string> >::iterator i = pairs.begin(); i != pairs.end(); ++i) pcre_free((*i).first);
}

combiner& combiner::init()
{
  this->out = 0;
  for(vector<pair<pcre*, string> >::iterator i = pairs.begin(); i != pairs.end(); ++i) { pcre_free((*i).first); }
  pairs.clear();
  first_row = 1;
  column = 0;
  remap_indexes.clear();
  tokens.clear();
  return *this;
}

combiner& combiner::init(pass& out) { init(); return set_out(out); }
combiner& combiner::set_out(pass& out) { this->out = &out; return *this; }

combiner& combiner::add_pair(const char* from, const char* to)
{
  const char* err; int err_off; pcre* p = pcre_compile(from, 0, &err, &err_off, 0);
  if(!p) throw runtime_error("combiner can't compile regex");
  pairs.resize(pairs.size() + 1);
  pairs.back().first = p;
  pairs.back().second = to;
  return *this;
}

void combiner::process_token(const char* token)
{
  if(first_row) {
    if(!out) throw runtime_error("combiner has no out");
    tokens.push_back(token);
  }
  else if(token[0]) {
    const int index = remap_indexes[column];
    if(!tokens[index].size()) tokens[index] = token;
    else throw runtime_error("multiple values for combiner");
  }

  ++column;
}

void combiner::process_line()
{
  if(first_row) {
    if(!out) throw runtime_error("combiner has no out");

    //create remap_indexes with gaps
    remap_indexes.resize(tokens.size());
    for(size_t i = 0; i < remap_indexes.size(); ++i) remap_indexes[i] = i;

    char* buf = new char[2048];
    char* end = buf + 2048;
    for(vector<pair<pcre*, string> >::iterator i = pairs.begin(); i != pairs.end(); ++i) {
      for(size_t j = 0; j < tokens.size(); ++j) {
        int ovector[30]; int rc = pcre_exec((*i).first, 0, tokens[j].c_str(), tokens[j].size(), 0, 0, ovector, 30);
        if(rc < 0) { if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("summarizer match error"); }
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
      out->process_token(tokens[distance(remap_indexes.begin(), it)].c_str());
    }

    //setup for second line
    tokens.resize(max_out_index + 1);
    for(vector<string>::iterator i = tokens.begin(); i != tokens.end(); ++i) (*i).clear();
    first_row = 0;
  }
  else {
    for(vector<string>::iterator i = tokens.begin(); i != tokens.end(); ++i) {
      out->process_token((*i).c_str());
      (*i).clear();
    }
  }
  out->process_line();
  column = 0;
}

void combiner::process_stream()
{
  if(!out) throw runtime_error("combiner has no out");
  out->process_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// writer
////////////////////////////////////////////////////////////////////////////////////////////////

void csv_writer_base::base_init()
{
  out = 0;
  line = 0;
  num_columns = 0;
  column = 0;
}

void csv_writer_base::process_token(const char* token)
{
  if(column) out->sputc(',');
  else if(!out) throw runtime_error("csv_writer has no out");

  for(const char* p = token; *p; ++p)
    out->sputc(*p);

  ++column;
}

void csv_writer_base::process_line()
{
  if(!line) {
    if(!out) throw runtime_error("csv_writer has no out");
    num_columns = column;
  }
  else if(column != num_columns) {
    stringstream msg; msg << "csv_writer: line " << line << " (zero's based) has " << column << " columns instead of " << num_columns;
    throw runtime_error(msg.str());
  }

  out->sputc('\n');
  column = 0;
  ++line;
}

csv_writer::csv_writer() { init(); }
csv_writer::csv_writer(streambuf* out) { init(out); }
csv_writer& csv_writer::init() { base_init(); return *this; }
csv_writer& csv_writer::init(streambuf* out) { init(); return set_out(out); }
csv_writer& csv_writer::set_out(streambuf* out) { if(!out) throw runtime_error("csv_writer::set_out null out"); this->out = out; return *this; }

void csv_writer::process_stream()
{
#ifdef TABLE_DIMENSIONS_DEBUG_PRINTS
  cerr << "csv_writer saw dimensions of " << num_columns << " by " << line << endl;
#endif
}

csv_file_writer::csv_file_writer() { init(); }
csv_file_writer::csv_file_writer(const char* filename) { init(filename); }
csv_file_writer& csv_file_writer::init() { delete out; base_init(); return *this; }
csv_file_writer& csv_file_writer::init(const char* filename) { init(); return set_out(filename); }

csv_file_writer& csv_file_writer::set_out(const char* filename)
{
  if(!filename) throw runtime_error("csv_file_writer::set_out null filename");

  if(!out) out = new filebuf;
  filebuf* o = static_cast<filebuf*>(out);
  if(o->is_open()) o->close();
  if(!o->open(filename, ios_base::out | ios_base::trunc))
    throw runtime_error("csv_file_writer can't open file");

  return *this;
}

csv_file_writer::~csv_file_writer() { delete out; }

void csv_file_writer::process_stream() {
#ifdef TABLE_DIMENSIONS_DEBUG_PRINTS
  cerr << "csv_file_writer saw dimensions of " << num_columns << " by " << line << endl;
#endif
  delete out; out = 0;
}


////////////////////////////////////////////////////////////////////////////////////////////////
// driver
////////////////////////////////////////////////////////////////////////////////////////////////

void read_csv(streambuf* in, pass& out)
{
  if(!in) throw runtime_error("read_csv got a null in");

  size_t line = 0;
  size_t num_keys = 0;
  size_t column = 0;
  bool blank = 1;
  char* buf = new char[4096];
  try {
    char* next = buf;
    char* end = buf + 4096;
    while(1) {
      int c = in->sbumpc();
      if(char_traits<char>::eof() == c) break;
      else if(c == '\n') {
        if(!blank) {
          if(next >= end) resize_buffer(buf, next, end);
          *next++ = '\0';
          out.process_token(buf);
          next = buf;
          ++column;
          if(!line) { num_keys = column; }
          else if(column != num_keys) throw runtime_error("didn't get same number of tokens as first line");
          out.process_line();
          ++line;
          column = 0;
          blank = 1;
        }
      }
      else if(c == ',') {
        if(next >= end) resize_buffer(buf, next, end);
        *next++ = '\0';
        out.process_token(buf);
        next = buf;
        ++column;
        blank = 0;
      }
      else {
        if(next >= end) resize_buffer(buf, next, end);
        *next++ = c;
        blank = 0;
      }
    }
#ifdef TABLE_DIMENSIONS_DEBUG_PRINTS
    cerr << "read_csv saw dimensions of " << num_keys << " by " << line << endl;
#endif
    out.process_stream();
  }
  catch(...) {
    delete[] buf;
    throw;
  }
  delete[] buf;
}

void read_csv(const char* filename, pass& out)
{
  filebuf f;
  if(!f.open(filename, ios_base::in)) throw runtime_error("didn't open file for reading");
  read_csv(&f, out);
}

}
