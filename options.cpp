using namespace std;

#include "stdafx.h"
#include "args.hxx"
#include "options.h"

int options::parse( int argc, char *argv[] )
{
  args::ArgumentParser parser("lsm-emu_parser.", "");
  args::Group group1(parser, "This group is all exclusive:", args::Group::Validators::DontCare);

  // path info
  args::ValueFlag<std::string> out_path_cmd(group1, "log", "path for log files", {'l', "log"}); 
  args::ValueFlag<std::string> db_path_cmd(group1, "path", "path for writing the DB and all the metadata files", {'p', "path"}); 
  args::ValueFlag<std::string> insert_path_cmd(group1, "insert_workload", "path for insert workload files", {'i', "insert_workload"});
  args::ValueFlag<std::string> query_path_cmd(group1, "query_workload", "path for query workload files", {'q', "query_workload"});

  args::ValueFlag<int> size_ratio_cmd(group1, "T", "The size ratio of two adjacent levels  [def: 2]", {'T', "size_ratio"});
  args::ValueFlag<int> buffer_size_in_pages_cmd(group1, "P", "The number of pages that can fit into a buffer [def: 1024]", {'P', "buffer_size_in_pageas"});
  args::ValueFlag<int> entries_per_page_cmd(group1, "B", "The number of entries that fit into a page [def: 128]", {'B', "entries_per_page"});
  args::ValueFlag<int> entry_size_cmd(group1, "E", "The size of a key-value pair inserted into DB [def: 64 B]", {'E', "entry_size"});
  args::ValueFlag<int> key_size_cmd(group1, "K", "The size of a key inserted into DB [def: 16 B]", {'K', "key_size"});

  args::ValueFlag<int> bits_per_key_cmd(group1, "bits_per_key", "The number of bits per key assigned to Bloom filter [def: 10]", {'b', "bits_per_key"});
  args::Flag fastlocal_bf_cmd(group1, "fastlocal_bf", "Enable FastLocal Bloom filters. [def: false]", {"FLBF", "enable_fast_local_filters"});
  args::ValueFlag<std::string> hash_type_cmd(group1, "Hash Type", "Hash type MM64, XXHash, CRC or CITY [def: MM64]", {"HT", "hash_type"});
  args::ValueFlag<int> num_filterunits_cmd(group1, "num_funit", "The number of filter units for modular filter. [def: 2] ", {"num_funit", "num_filter_units"});

  args::Flag share_hash_across_levels_cmd(group1, "shared_hash_level", "Enable sharing hash across levels ", {"lvl_share_hash", "enable_leveled_shared_hashing"});
  args::Flag share_hash_across_filter_units_cmd(group1, "shared_hash_filter_units", "Enable sharing hash across filter units ", {"funits_share_hash", "enable_filter_units_shared_hashing"});

  args::Flag destroy_db_cmd(group1, "destroy_db", "Delete the exsiting DB.", {"dd", "destroy_db"});

  args::ValueFlag<int> tries_cmd(group1, "number_of_tries", "#Tries to run the experiment (measured statistics would be averaged across #tries [def: 5]", { "tries"});
  args::ValueFlag<int> delay_cmd(group1, "delay", "Read delay (nanos)  added by human[def: 0]", {'D', "delay"});
  args::Flag direct_IO_cmd(group1, "directIO", "Enable direct IO (DO NOT Enable this using RAM disk!)", {"DIO", "direct_IO"});

  args::Flag block_cache_cmd(group1, "BlockCache", "Enable Block Cache", {"bc", "block_cache"});
  args::ValueFlag<int> block_cache_size_cmd(group1, "BlockCacheSize", "BlockCacheSize[def: 1KB]", {"bcs", "block_cache_size"});
  args::ValueFlag<int> block_cache_block_cache_priority_cmd(group1, "BlockCachePriority", "BlockCachePriority[def: 1]", {"bcp", "block_cache_pri"});

  args::Flag partitioned_cmd(group1, "partitioned_filter", "Make partitioned filter/index. ", {"part", "partitioned"});
  args::Flag monkey_cmd(group1, "monkey modules", "monkey modules", {"s", "monkey"});
  args::Flag skip_cmd(group1, "skip modules", "skip modules", {"s", "skip"});
  args::ValueFlag<int> skip_number_cmd(group1, "skip number", "skip number", {"sn", "skip_number"});
  args::ValueFlag<float> skip_th_0_cmd(group1, "skip threhold 0", "skip threshold 0", {"st0", "skip_th_0"});
  args::ValueFlag<float> skip_th_1_cmd(group1, "skip threhold 1", "skip threshold 1", {"st1", "skip_th_1"});

  args::Flag warmup_cmd(group1, "enable block cache warmup", "enable block cache warmup", {"w", "warmup"});
  args::Flag debug_cmd(group1, "debug", "debug", {"debug", "debug"});

  args::ValueFlag<float> propotional_cmd(group1, "MBF propotioanl size", "MBF propotional size[def: 1]", {"prop", "propotional"});

  try {
      parser.ParseCLI(argc, argv);
  }
  catch (args::Help&) {
      std::cout << parser;
      exit(0);
      // return 0;
  }
  catch (args::ParseError& e) {
      std::cerr << e.what() << std::endl;
      std::cerr << parser;
      return 1;
  }
  catch (args::ValidationError& e) {
      std::cerr << e.what() << std::endl;
      std::cerr << parser;
      return 1;
  }

  // path info

  db_path = db_path_cmd? args::get(db_path_cmd) : "./";
  destroy_db = destroy_db_cmd? args::get(destroy_db_cmd) : false;
  
  out_path = out_path_cmd? args::get(out_path_cmd) : "./out/";
  insert_path = insert_path_cmd? args::get(insert_path_cmd) : "base_1.0_workload.txt";
  query_path = query_path_cmd? args::get(query_path_cmd) : "Z0_zipfian.workload.txt";

  // basic LSM setting
  size_ratio = size_ratio_cmd? args::get(size_ratio_cmd) : 2;
  buffer_size_in_pages = buffer_size_in_pages_cmd? args::get(buffer_size_in_pages_cmd) : 1024;
  entries_per_page = entries_per_page_cmd? args::get(entries_per_page_cmd) : 64;
  entry_size = entry_size_cmd? args::get(entry_size_cmd) : 64;
  key_size = key_size_cmd? args::get(key_size_cmd) : 16;

  bits_per_key = bits_per_key_cmd? args::get(bits_per_key_cmd) : 10;
  fastlocal_bf = fastlocal_bf_cmd? args::get(fastlocal_bf_cmd) : false;
  hash_type = hash_type_cmd? args::get(hash_type_cmd) : "MM64";
  num_filterunits = num_filterunits_cmd? args::get(num_filterunits_cmd) : 2;

  share_hash_across_levels = share_hash_across_levels_cmd ? args::get(share_hash_across_levels_cmd): false;
  share_hash_across_filter_units = share_hash_across_filter_units_cmd ? args::get(share_hash_across_filter_units_cmd) : false;

  directIO = direct_IO_cmd ? args::get(direct_IO_cmd) : false;

  block_cache = block_cache_cmd ? args::get(block_cache_cmd) : false;
  block_cache_size = block_cache_size_cmd ? args::get(block_cache_size_cmd) : 1024 * 8;
  block_cache_priority = block_cache_block_cache_priority_cmd? args::get(block_cache_block_cache_priority_cmd) : 1;

  partitioned = partitioned_cmd? args::get(partitioned_cmd) : false;
  monkey = monkey_cmd? args::get(monkey_cmd) : false;
  skip = skip_cmd? args::get(skip_cmd) : false;
  skip_number = skip_number_cmd? args::get(skip_number_cmd) : 0;
  skip_th_0 = skip_th_0_cmd? args::get(skip_th_0_cmd) : 0.1;
  skip_th_1 = skip_th_1_cmd? args::get(skip_th_1_cmd) : 0.1;

  warmup = warmup_cmd? args::get(warmup_cmd) : false;
  debug = debug_cmd? args::get(debug_cmd) : false;

  tries = tries_cmd ? args::get(tries_cmd) : 5;
  if(tries <= 0){
    cout << "Warning: tries has to be a positive number. Reset to the default number 5" << endl;
    tries = 5;
  }
  delay = delay_cmd ? args::get(delay_cmd) : 0;

  propotional = propotional_cmd ? args::get(propotional_cmd) : 1;
  if( propotional <= 0 & propotional>1 ){
    cout << "Warning: propotional has to be 0 < propotional <= 1. Reset to the default value 1" << endl;
    propotional = 1;
  }
  return 0;
}
