#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include "table.h"

#undef TABLE_DIMENSIONS_DEBUG_PRINTS

using namespace std;
using namespace std::tr1;


namespace table {

void resize_buffer(char*& buf, char*& next, char*& end, size_t min_to_add, char** resize_end)
{
  char* old = buf;
  size_t size = next - buf;
  size_t cap = end - buf;
  size_t resize = resize_end ? end - *resize_end : 0;

  size_t new_cap = cap * 2;
  if(size + min_to_add > new_cap) new_cap = size + min_to_add;
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
  const double thres = (double)(0x7FFFFFFF);
  if(value > thres || value < -thres) { return sprintf(str, "%.6g", value); }

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
// pass
////////////////////////////////////////////////////////////////////////////////////////////////

pass::~pass() {}

void pass::process_token(double token)
{
  char buf[32];
  size_t len = dtostr(token, buf);
  process_token(buf, len);
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
      else { size_t len = strlen(cur); t->out->process_token(cur, len); cur += len; }
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

void threader::process_token(const char* token, size_t len)
{
  if(!thread_created) {
    if(!out) throw runtime_error("threader has no out");
    pthread_mutex_init(&mutex, 0);
    pthread_cond_init(&prod_cond, 0);
    pthread_cond_init(&cons_cond, 0);
    pthread_create(&thread, 0, threader_main, this);
    thread_created = 1;
  }

  if(size_t(chunks[write_chunk].end - write_chunk_next) < len + 2) {
    if(write_chunk_next == chunks[write_chunk].start) resize_write_chunk(len + 2);
    else inc_write_chunk();
  }
  memcpy(write_chunk_next, token, len); write_chunk_next += len;
  *write_chunk_next++ = '\0';
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

void subset_tee::process_token(const char* token, size_t len)
{
  if(first_row) {
    for(std::map<pass*, subset_tee_dest_data_t>::iterator di = dest_data.begin(); di != dest_data.end(); ++di) {
      subset_tee_dest_data_t& d = (*di).second;

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

void subset_tee::process_token(double token)
{
  if(first_row) { char buf[32]; size_t len = dtostr(token, buf); process_token(buf, len); return; }

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

void ordered_tee::process_token(const char* token, size_t len)
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

void stacker::resize(size_t len, std::vector<std::pair<char*, char*> >& tokens, size_t& index, char*& next)
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

void stacker::process_leave_tokens()
{
  vector<pair<char*, char*> >::iterator lti = leave_tokens.begin();
  if(lti == leave_tokens.end()) return;
  for(char* ltp = (*lti).first; 1;) {
    if(*ltp == '\x01') { out->process_token(*reinterpret_cast<double*>(++ltp)); ltp += sizeof(double); }
    else if(*ltp == '\x03') ltp = (*++lti).first;
    else if(*ltp == '\x04') break;
    else { size_t len = strlen(ltp); out->process_token(ltp, len); ltp += len + 1; }
  }
}

void stacker::process_stack_tokens()
{
  vector<pair<char*, char*> >::iterator sti = stack_tokens.begin();
  stack_column = 0;
  for(char* stp = (*sti).first; 1;) {
    process_leave_tokens();
    out->process_token(stack_keys[stack_column].c_str(), stack_keys[stack_column].size());
    if(*stp == '\x01') { out->process_token(*reinterpret_cast<double*>(++stp)); stp += sizeof(double); }
    else { size_t len = strlen(stp); out->process_token(stp, len); stp += len + 1; }
    out->process_line();
    ++stack_column;
    if(*stp == '\x03') stp = (*++sti).first;
    else if(*stp == '\x04') break;
  }
}

stacker::stacker(stack_action_e default_action) { init(default_action); }
stacker::stacker(pass& out, stack_action_e default_action) { init(out, default_action); }

stacker::~stacker()
{
  for(vector<regex_stack_action_t>::iterator i = regex_actions.begin(); i != regex_actions.end(); ++i) pcre_free((*i).regex);
  for(vector<pair<char*, char*> >::iterator i = leave_tokens.begin(); i != leave_tokens.end(); ++i) delete[] (*i).first;
  for(vector<pair<char*, char*> >::iterator i = stack_tokens.begin(); i != stack_tokens.end(); ++i) delete[] (*i).first;
}

stacker& stacker::re_init()
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
  return *this;
}

stacker& stacker::init(stack_action_e default_action)
{
  this->out = 0;
  this->default_action = default_action;
  keyword_actions.clear();
  for(vector<regex_stack_action_t>::iterator i = regex_actions.begin(); i != regex_actions.end(); ++i) pcre_free((*i).regex);
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

void stacker::process_token(const char* token, size_t len)
{
  if(first_line) {
    if(!out) throw runtime_error("stacker has no out");
    stack_action_e action = default_action;
    map<string, stack_action_e>::iterator i = keyword_actions.find(token);
    if(i != keyword_actions.end()) action = (*i).second;
    else {
      for(vector<regex_stack_action_t>::iterator j = regex_actions.begin(); j != regex_actions.end(); ++j) {
        int ovector[30]; int rc = pcre_exec((*j).regex, 0, token, len, 0, 0, ovector, 30);
        if(rc >= 0) { action = (*j).action; break; }
        else if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("stacker match error");
      }
    }
    actions.push_back(action);
    if(action == ST_LEAVE) { last_leave = column; out->process_token(token, len); }
    else if(action == ST_STACK) stack_keys.push_back(token);
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
        out->process_token(stack_keys[stack_column].c_str(), stack_keys[stack_column].size());
        out->process_token(token, len);
        out->process_line();
        ++stack_column;
      }
    }
  }

  ++column;
}

void stacker::process_token(double token)
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
      out->process_token(stack_keys[stack_column].c_str(), stack_keys[stack_column].size());
      out->process_token(token);
      out->process_line();
      ++stack_column;
    }
  }

  ++column;
}

void stacker::process_line()
{
  if(first_line) {
    if(!out) throw runtime_error("stacker has no out");
    out->process_token("keyword", 7);
    out->process_token("data", 4);
    out->process_line();
    first_line = 0;
  }
  column = 0;
  stack_column = 0;
  leave_tokens_index = 0;
  leave_tokens_next = 0;
  stack_tokens_index = 0;
  stack_tokens_next = 0;
}

void stacker::process_stream()
{
  if(!out) throw runtime_error("stacker has no out");

  for(vector<pair<char*, char*> >::iterator i = leave_tokens.begin(); i != leave_tokens.end(); ++i) delete[] (*i).first;
  leave_tokens.clear();
  for(vector<pair<char*, char*> >::iterator i = stack_tokens.begin(); i != stack_tokens.end(); ++i) delete[] (*i).first;
  stack_tokens.clear();

  out->process_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// splitter
////////////////////////////////////////////////////////////////////////////////////////////////

splitter::splitter(split_action_e default_action) : group_tokens(0), split_by_tokens(0), split_tokens(0) { init(default_action); }
splitter::splitter(pass& out, split_action_e default_action) : group_tokens(0), split_by_tokens(0), split_tokens(0)  { init(out, default_action); }

splitter::~splitter()
{
  for(vector<regex_action_t>::const_iterator i = regex_actions.begin(); i != regex_actions.end(); ++i) pcre_free((*i).regex);
  delete[] group_tokens;
  delete[] split_by_tokens;
  delete[] split_tokens;
  for(map<char*, size_t, cstr_less>::const_iterator i = out_split_keys.begin(); i != out_split_keys.end(); ++i) delete[] (*i).first;
  for(vector<char*>::const_iterator i = group_storage.begin(); i != group_storage.end(); ++i) delete[] *i;
}


splitter& splitter::init(split_action_e default_action)
{
  this->out = 0;
  this->default_action = default_action;
  keyword_actions.clear();
  regex_actions.clear();

  first_line = 1;
  for(vector<regex_action_t>::const_iterator i = regex_actions.begin(); i != regex_actions.end(); ++i) pcre_free((*i).regex);
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

  for(map<char*, size_t, cstr_less>::const_iterator i = out_split_keys.begin(); i != out_split_keys.end(); ++i) delete[] (*i).first;
  for(vector<char*>::const_iterator i = group_storage.begin(); i != group_storage.end(); ++i) delete[] *i;
  group_storage_next = 0;
  group_storage_end = 0;
  out_split_keys.clear();
  data.clear();

  return *this;
}

splitter& splitter::init(pass& out, split_action_e default_action) { init(default_action); return set_out(out); }
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

void splitter::process_token(const char* token, size_t len)
{
  if(first_line) {
    if(!out) throw runtime_error("splitter has no out");
    split_action_e action = default_action;
    map<string, split_action_e>::iterator i = keyword_actions.find(token);
    if(i != keyword_actions.end()) action = (*i).second;
    else {
      for(vector<regex_action_t>::iterator j = regex_actions.begin(); j != regex_actions.end(); ++j) {
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

void splitter::process_stream()
{
  if(!out) throw runtime_error("splitter has no out");

  for(vector<string>::const_iterator i = group_keys.begin(); i != group_keys.end(); ++i)
    out->process_token((*i).c_str(), (*i).size());
  {
    vector<char*> osk(out_split_keys.size());
    for(map<char*, size_t, cstr_less>::const_iterator i = out_split_keys.begin(); i != out_split_keys.end(); ++i)
      osk[(*i).second] = (*i).first;
    for(vector<char*>::const_iterator i = osk.begin(); i != osk.end(); ++i)
      out->process_token(*i, strlen(*i));
  }
  out->process_line();

  map<char*, size_t, cstr_less>::size_type num_out_split_keys = out_split_keys.size();
  for(map<char*, size_t, cstr_less>::const_iterator i = out_split_keys.begin(); i != out_split_keys.end(); ++i)
    delete[] (*i).first;
  out_split_keys.clear();

  for(data_t::const_iterator i = data.begin(); i != data.end(); ++i) {
    for(char* p = (*i).first; *p != '\x03';) {
      size_t len = strlen(p);
      out->process_token(p, len);
      p += len + 1;
    }
    size_t tokens = 0;
    for(vector<string>::const_iterator j = (*i).second.begin(); j != (*i).second.end(); ++j, ++tokens)
      out->process_token((*j).c_str(), (*j).size());
    while(tokens++ < num_out_split_keys)
      out->process_token("", 0);
    out->process_line();
  }

  for(vector<char*>::const_iterator i = group_storage.begin(); i != group_storage.end(); ++i) delete[] *i;
  group_storage.clear();
  data.clear();

  out->process_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// sorter
////////////////////////////////////////////////////////////////////////////////////////////////

sorter::sorter() : sort_buf(0), sort_buf_end(0) { init(); }
sorter::sorter(pass& out) : sort_buf(0), sort_buf_end(0) { init(out); }

sorter& sorter::init()
{
  this->out = 0;
  columns.clear();
  sorts_found = 0;
  first_line = 1;
  delete[] sort_buf_end; sort_buf_end = 0;
  if(sort_buf) { for(size_t i = 0; i < sorts.size(); ++i) delete[] sort_buf[i]; }
  delete[] sort_buf; sort_buf = 0;
  sorts.clear();
  for(vector<char*>::const_iterator i = sort_storage.begin(); i != sort_storage.end(); ++i) delete[] *i;
  sort_storage.clear();
  sort_next = 0;
  sort_end = 0;
  for(vector<char*>::const_iterator i = other_storage.begin(); i != other_storage.end(); ++i) delete[] *i;
  other_storage.clear();
  other_next = 0;
  other_end = 0;
  rows.clear();
  return *this;
}

sorter& sorter::init(pass& out) { init(); return set_out(out); } 

sorter::~sorter() {
  delete[] sort_buf_end;
  if(sort_buf) { for(size_t i = 0; i < sorts.size(); ++i) delete[] sort_buf[i]; }
  delete[] sort_buf;
  for(vector<char*>::const_iterator i = sort_storage.begin(); i != sort_storage.end(); ++i) delete[] *i;
  for(vector<char*>::const_iterator i = other_storage.begin(); i != other_storage.end(); ++i) delete[] *i;
}

sorter& sorter::set_out(pass& out) { this->out = &out; return *this; }

sorter& sorter::add_sort(const char* key, bool ascending)
{
  for(vector<sorts_t>::const_iterator i = sorts.begin(); i != sorts.end(); ++i)
    if(!(*i).key.compare(key))
      throw runtime_error("sorter has sort already");
  sorts.resize(sorts.size() + 1);
  sorts.back().key = key;
  sorts.back().type = ascending ? 1 : 2;
  return *this;
}

void sorter::process_token(const char* token, size_t len)
{
  size_t index;

  if(first_line) {
    if(!out) throw runtime_error("sorter has no out");

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

void sorter::process_line()
{
  if(first_line) {
    if(!out) throw runtime_error("sorter has no out");
    if(sorts_found < sorts.size()) throw runtime_error("sorter didn't find enough columns");
    if(sorts_found > sorts.size()) throw runtime_error("sorter didn't find too many sort columns");
    first_line = 0;

    for(size_t i = 0; i < sorts.size(); ++i) {
      out->process_token(sort_buf[i], strlen(sort_buf[i]));
      sort_buf[i][0] = '\0';
    }

    *other_next++ = '\x03';
    vector<char*>::iterator osi = other_storage.begin();
    char* p = osi == other_storage.end() ? 0 : *osi;
    while(1) {
      const char* s = p;
      while(*p) ++p;
      out->process_token(s, p - s);
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
    out->process_line();
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

struct sorter_compare {
  const vector<sorter::sorts_t>& sorts;

  sorter_compare(const vector<sorter::sorts_t>& sorts) : sorts(sorts) {}

  bool operator() (const sorter::row_t& lhs, const sorter::row_t& rhs) {
    vector<sorter::sorts_t>::const_iterator i = sorts.begin();
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
};

void sorter::process_stream()
{
  if(!out) throw runtime_error("sorter has no out");

  if(sort_next) *sort_next++ = '\x04';
  if(other_next) *other_next++ = '\x04';

  sorter_compare comp(sorts);
  sort(rows.begin(), rows.end(), comp);

  for(vector<row_t>::iterator ri = rows.begin(); ri != rows.end(); ++ri) {
    for(const char* p = (*ri).sort; *p != '\x03';) {
      size_t len = strlen(p);
      out->process_token(p, len);
      p += len + 1;
    }
    size_t index = (*ri).other_index;
    for(const char* p = (*ri).other; *p != '\x03';) {
      size_t len = strlen(p);
      out->process_token(p, len);
      p += len + 1;
      if(*p == '\x04') {
        p = other_storage[++index];
      }
    }
    out->process_line();
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

  out->process_stream();
}


////////////////////////////////////////////////////////////////////////////////////////////////
// row_joiner
////////////////////////////////////////////////////////////////////////////////////////////////

row_joiner::row_joiner() : out(0) {}
row_joiner::row_joiner(pass& out, const char* table_name) { init(out, table_name); }

row_joiner::~row_joiner()
{
  for(vector<data_t>::iterator i = data.begin(); i != data.end(); ++i)
    for(vector<char*>::iterator j = (*i).data.begin(); j != (*i).data.end(); ++j)
      delete[] *j;
}

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
  for(vector<data_t>::iterator i = data.begin(); i != data.end(); ++i)
    for(vector<char*>::iterator j = (*i).data.begin(); j != (*i).data.end(); ++j)
      delete[] *j;
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

void row_joiner::process_token(const char* token, size_t len)
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

void row_joiner::process_token(double token)
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

void row_joiner::process_line()
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
      out->process_token((*ki).c_str(), (*ki).size());
    }
    out->process_line();
  }

  for(vector<data_t>::iterator di = data.begin(); di != data.end(); ++di) {
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
    for(vector<data_t>::iterator di = data.begin(); di != data.end(); ++di, ++nci) { // table loop
      data_t& d = *di;
      for(size_t col = 0; col < (*nci); ++col) {
        if(*d.next == '\x03') {
          if(++d.i != d.data.end()) d.next = *d.i;
          else { d.next = 0; done = 1; break; }
        }
        if(*d.next == '\x01') {
          out->process_token(*reinterpret_cast<const double*>(++d.next));
          d.next += sizeof(double);
        }
        else {
          size_t len = strlen(d.next);
          out->process_token(d.next, len);
          d.next += len + 1;
        }
      }
    }
    if(!done) out->process_line();
  }

  table = 0;
  first_line = 1;
  more_lines = 1;
  for(vector<data_t>::iterator i = data.begin(); i != data.end(); ++i)
    for(vector<char*>::iterator j = (*i).data.begin(); j != (*i).data.end(); ++j)
      delete[] *j;
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

void col_pruner::process_token(const char* token, size_t len)
{
  if(passthrough) { out->process_token(token, len); return; }

  if(first_row) {
    if(!out) throw runtime_error("col_pruner has no out");
  }
  else if(token[0]) {
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
          size_t len = strlen(p);
          out->process_token(p, len);
          p += len + 1;;
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
          size_t len = strlen(p);
          out->process_token(p, len);
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

void combiner::process_token(const char* token, size_t len)
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
      out->process_token(tokens[distance(remap_indexes.begin(), it)].c_str(), tokens[distance(remap_indexes.begin(), it)].size());
    }

    //setup for second line
    tokens.resize(max_out_index + 1);
    for(vector<string>::iterator i = tokens.begin(); i != tokens.end(); ++i) (*i).clear();
    first_row = 0;
  }
  else {
    for(vector<string>::iterator i = tokens.begin(); i != tokens.end(); ++i) {
      out->process_token((*i).c_str(), (*i).size());
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
// substituter
////////////////////////////////////////////////////////////////////////////////////////////////

substituter::substituter() : data(0) {}
substituter::substituter(const char* from, const char* to) : data(0) { init(from, to); }
substituter::substituter(const substituter& other) { data = other.data; if(data) ++data->rc; }

substituter::~substituter()
{
  if(!data) return;

  if(!--data->rc) {
    pcre_free_study(data->from_extra);
    pcre_free(data->from);
    delete[] data->buf;
    delete data;
  }
}

void substituter::operator=(const substituter& other)
{
  if(data && !--data->rc) {
    pcre_free_study(data->from_extra);
    pcre_free(data->from);
    delete[] data->buf;
    delete data;
  }

  data = other.data;
  if(data) ++data->rc;
}

void substituter::init(const char* from, const char* to)
{
  if(data) {
    if(data->rc <= 1) {
      pcre_free_study(data->from_extra);
      pcre_free(data->from);
    }
    else { --data->rc; data = new data_t; data->buf = new char[2048]; data->end = data->buf + 2048; data->rc = 1; }
  }
  else { data = new data_t; data->buf = new char[2048]; data->end = data->buf + 2048; data->rc = 1; }

  const char* err; int err_off;
  data->from = pcre_compile(from, 0, &err, &err_off, 0);
  if(!data->from) throw runtime_error("substitutor can't compile from regex");

  data->from_extra = pcre_study(data->from, PCRE_STUDY_JIT_COMPILE, &err);
  if(!data->from_extra) throw runtime_error("substitutor can't study from");

  data->to = to;
}

c_str_and_len_t substituter::operator()(c_str_and_len_t in)
{
  c_str_and_len_t ret(in);

  int ovector[30]; int rc = pcre_exec(data->from, data->from_extra, in.c_str, in.len, 0, 0, ovector, 30);
  if(rc < 0) { if(rc != PCRE_ERROR_NOMATCH) throw runtime_error("substitutor exception match error"); }
  else {
    char* next = data->buf;
    if(!rc) rc = 10;
    generate_substitution(in.c_str, data->to.c_str(), ovector, rc, data->buf, next, data->end);
    ret.c_str = data->buf;
    ret.len = next - data->buf - 1;
  }

  return ret;
}


////////////////////////////////////////////////////////////////////////////////////////////////
// tabular_writer
////////////////////////////////////////////////////////////////////////////////////////////////

void tabular_writer::process_data()
{
  if(!next) return;

  *next++ = '\03';

  size_t c = 0;
  char buf[32];
  for(; c < max_width.size(); ++c) {
    int len = sprintf(buf, "c%zu", c);
    size_t spaces = 1 + max_width[c] - len;

    for(size_t i = 0; i < spaces; ++i)
      out->sputc(' ');

    out->sputn(buf, len);
  }
  out->sputc('\n');

  c = 0;
  for(vector<char*>::iterator i = data.begin(); i != data.end(); ++i) {
    next = *i;
    while(*next != '\03') {
      size_t len = strlen(next);
      size_t spaces = 1 + max_width[c] - len;

      for(size_t i = 0; i < spaces; ++i)
        out->sputc(' ');

      out->sputn(next, len);
      next += len + 1;

      if(++c >= max_width.size()) {
        out->sputc('\n');
        c = 0;
      }
    }
    delete[] *i;
  }
  max_width.clear();
  data.clear();
}

tabular_writer::tabular_writer() { init(); }
tabular_writer::tabular_writer(streambuf* out) { init(out); }

tabular_writer& tabular_writer::init()
{
  this->out = 0;
  line = 0;
  column = 0;
  max_width.clear();
  for(vector<char*>::iterator i = data.begin(); i != data.end(); ++i) delete[] *i;
  data.clear();
  next = 0;
  end = 0;
  return *this;
}

tabular_writer& tabular_writer::init(streambuf* out) { init(); return set_out(out); }
tabular_writer& tabular_writer::set_out(streambuf* out) { if(!out) throw runtime_error("tabular_writer::null out"); this->out = out; return *this; }

void tabular_writer::process_token(const char* token, size_t len)
{
  if(!line) {
    if(!next || next + len + 20 > end) {
      size_t cap = 256 * 1024;
      if(cap < len + 20) cap = len + 20;
      data.push_back(new char[cap]);
      next = data.back();
      end = data.back() + cap;
    }
    next += sprintf(next, "c%zu", column);
    max_width.push_back(next - data.back());
    next += sprintf(next, "=%s\n", token);
    out->sputn(data.back(), next - data.back());
    next = data.back();
  }
  else if(line < 20) {
    if(max_width[column] < len) max_width[column] = len;
    if(!next || next + len + 2 > end) {
      if(next) *next++ = '\03';
      size_t cap = 256 * 1024;
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

    for(size_t i = 0; i < spaces; ++i)
      out->sputc(' ');

    out->sputn(token, len);
  }

  ++column;
}

void tabular_writer::process_line()
{
  if(!out) throw runtime_error("tabular_writer has no out");

  if(!line) out->sputc('\n');
  else if(line == 19) { process_data(); }
  else if(line > 19) out->sputc('\n');

  ++line;
  column = 0;
}


void tabular_writer::process_stream()
{
  if(line <= 19) { process_data(); }
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

void csv_writer_base::process_token(const char* token, size_t len)
{
  if(column) out->sputc(',');
  else if(!out) throw runtime_error("csv_writer has no out");

  out->sputn(token, len);

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
  if(line && column) throw runtime_error("csv_writer saw process_stream called after process_token");

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
  if(line && column) throw runtime_error("csv_file_writer saw process_stream called after process_token");

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
          *next = '\0';
          out.process_token(buf, next - buf);
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
        *next = '\0';
        out.process_token(buf, next - buf);
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
