#pragma once
using namespace std;

#include <string>

class options {
public:
  // path info
  string db_path;
  bool destroy_db;

  string out_path;
  string insert_path;
  string query_path;

  // basic LSM setting
  int size_ratio;
  int buffer_size_in_pages;
  int entries_per_page;
  int entry_size;
  int key_size;

  // BF related info
  int bits_per_key;
  int num_filterunits;
  bool fastlocal_bf;
  string hash_type;

  // share hash
  bool share_hash_across_levels;
  bool share_hash_across_filter_units;

  // experiments
  int tries;
  int delay;
  bool directIO;

  // cache related info
  bool block_cache;
  int block_cache_size;
  int block_cache_priority;

  // warmup
  int warmup_queries;
  bool warmup;

  bool partitioned = false;
  bool monkey = false;
  bool skip = false;
  int  skip_number = 0;
  float skip_th_0 = 0;
  float skip_th_1 = 0;

  bool debug = false;

  float propotional;

  int parse(int argc, char *argv[]);
};
