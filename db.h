
#pragma once
using namespace std;

#include "stdafx.h"

#include "options.h"
#include "BF_bit.h"
#include "PriorityCache.h"

class db
{
private:
    // log path
    string out_path;
    // db path
    string bf_dir;
    string index_dir;
    string data_dir;
    string settings_file;
    string fence_file;
    int file_read_flags;

    // db parameter
    int P;
    int B;
    int E;
    int K;
    int size_ratio;
    int buffer_size;

    int num_levels = 0;
    int num_sst = 0;
    vector <int> num_sstPerLevel;
    int last_sst_keys;

    // BF related
    int bpk;
    int mod_bf;
    bool monkey;
	vector<float> monkey_bpk = {20.5, 17.5, 14.8, 11.9, 9};
    bool skip;
    int  skip_number;
    vector<float> skip_th;
    vector<int> num_skip;
    vector <vector <int>> access_freq;
    vector <vector <int>> access_tp;

    bool fastlocal_bf;
    int num_filter_units;
    vector<int> filter_unit_bpk;
    vector<int> filter_unit_size;
    //vector<int> filter_unit_page;
    vector<int> filter_unit_byte;
    vector<int> filter_unit_index;
    vector<uint32_t> filter_unit_num_lines;
    //int filter_unit_size;
    //int filter_unit_byte;
    //int filter_unit_index;

    //int filter_size;
    //int last_filter_size;
    //uint32_t num_lines;
    //uint32_t last_num_lines;
    //int num_probes;

    bool partitioned;
    int  partitions;
    int  index_in_a_page = 0;

    int index_size;
    int data_size;

    vector<vector<string> > fence_pointers;
    char***** bf_prime;
    //char**** blk_fp_prime;
    vector<vector<vector<string>>> blk_fp_prime;

    int** blk_size_prime;
    vector<uint64_t> hash_digests;

    // block cache related
    bool block_cache_en;
    uint64_t block_cache_size;
    int block_cache_priority;
    PriorityCache * block_cache;

    uint64_t warm_size;

    int filter_pri = 0;
    int index_pri = 0;
    int data_pri = 0;

    unsigned long readbytes = 0;
    unsigned long total_io = 0;
    unsigned int bloom_hit = 0;
    unsigned int bloom_miss = 0;
    unsigned int index_hit = 0;
    unsigned int index_miss = 0;
    unsigned int data_hit = 0;
    unsigned int data_miss = 0;
    // block cache related end

    bool debug = false;

    float propotional = 1;

 public:
    int total = 0;

    bool warmup;
    // get starting timepoint
    fsec total_duration = std::chrono::microseconds::zero();
    fsec data_duration = std::chrono::microseconds::zero();
    fsec hash_duration = std::chrono::microseconds::zero();
    fsec bs_duration = std::chrono::microseconds::zero();

    uint64_t total_n = 0;
    uint64_t total_p = 0;
    uint64_t total_fp = 0;
    uint64_t total_skip = 0;
    uint64_t total_skip_fp = 0;
    vector<uint64_t> fp_vec;
    vector<uint64_t> n_vec;

    unsigned int num_lookups = 0;
    unsigned int lnum_single = 0;
    unsigned int qnum_single = 0;
    unsigned int lnum = 0;
    unsigned int qnum = 0;

    vector<int> positive_counters;
    vector<int>* qnum_histogram;
    vector<int>* lnum_histogram;

    // bf evaluating counter
    vector<int>* bf_eval_histogram;
    // bf true positive counter
    vector<int>* bf_tp_eval_histogram;
    //vector<int>* bf_eval_histogram = new vector<int> (num_levels, 0);
    //vector<int>* bf_tp_eval_histogram = new vector<int> (num_levels, 0);

    // experiments
    int tries;
    int delay;
    db( options op );

    void split_keys( vector<string> table_in, vector<vector<vector<string> > > & reallocated_keys );
    void split_keys_sst(int sst_capacity, vector<string> keys_one_level, vector<vector<string> > & sst, vector<string> & fence_pointer );

    void Build( vector<vector<vector<string>>> reallocated_keys, bool bf_only );
    int ReadSettings();

    string Get( string key, bool * result );
    string GetLevel( int i, BFHash & bfHash, string & key, bool * result );
    bool QueryFilter( int i, int bf_no, int partition, vector<uint64_t>* hash_digests, char** bf_list, bool * skipped);


    void read_bf(string filename, char* bf, int size);
    void flushBFfile(string filename, const char* filterunit, uint32_t size);	
    void flushfile( string file, vector<string>* table);
    void flushIndexes( string file, char** indexes, int size, int key_size);
    int  GetFromTopLvIndex( int l, int bf_no, string key );
    int  GetFromIndex( int i, int bf_no, int partition, string key);
    int  read_index_size(string filename);
    void read_index( string filename, int index_size, char** index);
    void read_index( string filename, int index_size, vector<string> & index);
    void read_index( string filename, int key_size, int index_pos, bool partial, vector<string> & index );
    int  read_data ( string filename, int pos, int key_size, vector<string> & data_block );
    string GetFromData( int i, int bf_no, int index_pos, string key, bool * result );

    int binary_search(string key, vector<string> & fence_pointer);
    int binary_search(string key, char** indexes, int size);

    void SetTreeParam();
    fsec loadBFAndIndex();

    void PrintStat(double program_total=0.0, double data_load_total=0.0);

    void SetBlockCache( options op );
    float calcFP( int blo );
};
