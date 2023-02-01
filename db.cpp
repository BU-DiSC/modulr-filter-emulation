using namespace std;

#include "db.h"
#include "FastLocalBF.h"
#include "LegacyBF.h"
#include <math.h>
#include <iostream>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <linux/fs.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/ioctl.h>

#define DB_PAGE_SIZE 4096
#define BYTE 8


db::db( options op )
{
    string db_path = op.db_path;
    out_path = op.out_path;

    bf_dir    = db_path + "/bf_home/";
    index_dir = db_path + "/index/";
    data_dir  = db_path + "/data/";
    
    settings_file = db_path + "settings.txt";
    fence_file = db_path + "fence.txt";
    
    string bf_command = "mkdir -p " + bf_dir;
    string data_command = "mkdir -p " + data_dir;
    string index_command = "mkdir -p " + index_dir;
    system(bf_command.c_str());
    system(data_command.c_str());
    system(index_command.c_str());
    cout << bf_command << endl;
    cout << data_command << endl;
    cout << index_command << endl;

    // basic LSM setting
    P = op.buffer_size_in_pages;
    B = op.entries_per_page;
    E = op.entry_size;
    K = op.key_size;
    size_ratio = op.size_ratio;
    fastlocal_bf = op.fastlocal_bf;

    buffer_size = P * B;

    partitioned = op.partitioned;
    index_in_a_page = floor(DB_PAGE_SIZE/K);
    partitions = ( partitioned==false )? 1 : ceil(P/index_in_a_page);

    index_size = P*K/partitions;
    data_size = B*E;

    // BF related
    bpk = op.bits_per_key;
    //mod_bf = op.elastic_filters;
    num_filter_units = op.num_filterunits;


    hash_digests = vector<uint64_t> (num_filter_units, 0);
    BFHash::hash_digests_ = new vector<uint64_t> (num_filter_units, 0);
    //experiment
    tries = op.tries;
    delay = op.delay;
    file_read_flags = O_RDWR | O_SYNC;
    if(op.directIO){
        file_read_flags = O_RDWR | O_DIRECT | O_SYNC;
    }

    //int bf_size = (buffer_size * bpk) / partitions;
    int bf_page = (int)round((float)((buffer_size*bpk)/partitions)/DB_PAGE_SIZE);
    int bf_size = bf_page * DB_PAGE_SIZE;
    int bf_index = (int)floor(0.693*bpk + 0.5);
    if ( propotional != 1 & num_filter_units != 2 ) {
        cout << "MBF propotional can be enabled only using 2 modules. propotional will be ignored." << endl;
        propotional = 1;
    }
    filter_unit_bpk.resize(num_filter_units, 0);
    filter_unit_size.resize(num_filter_units, 0);
    filter_unit_byte.resize(num_filter_units, 0);
    filter_unit_index.resize(num_filter_units, 0);
    filter_unit_num_lines.resize(num_filter_units, 0);

	monkey = op.monkey;
    if ( monkey ){
        bpk = monkey_bpk[0];
    }

    for ( int i=0 ; i<num_filter_units ; i++ ) {
        if ( propotional==1 ) {
            filter_unit_bpk[i]   = bpk/num_filter_units;
            //filter_unit_size[i]  = (int)round((float)(bf_size/num_filter_units)/DB_PAGE_SIZE)*DB_PAGE_SIZE;
            //filter_unit_byte[i]  = ceil(float(filter_unit_size[i])/BYTE);
            //filter_unit_index[i] = bf_index/num_filter_units;
    	}
    	else if ( i==0 ) {
                filter_unit_bpk[i]   = (int)(bpk * propotional);
                //filter_unit_size[i]  = (bf_page * propotional)*DB_PAGE_SIZE;
                //filter_unit_byte[i]  = ceil(float(filter_unit_size[i])/BYTE);
                //filter_unit_index[i] = bf_index * propotional;
    	}
    	else{
                filter_unit_bpk[i]   = bpk  - filter_unit_bpk[0];
                //filter_unit_size[i]  = bf_size - filter_unit_size[0];
                //filter_unit_byte[i]  = ceil(float(filter_unit_size[i])/BYTE);
                //filter_unit_index[i] = bf_index - filter_unit_index[0];
    	}
    	if(fastlocal_bf){
    		FastLocalBF tmpbf = FastLocalBF(filter_unit_bpk[i]);
    		filter_unit_byte[i] = tmpbf.CalculateSpace(B*P/partitions);
    		filter_unit_size[i] = filter_unit_byte[i]*BYTE;
    		filter_unit_index[i] = tmpbf.num_probes_;
    	}else{
    		LegacyBF tmpbf = LegacyBF( filter_unit_bpk[i] );
    		uint32_t dont_care;
    		filter_unit_byte[i] = tmpbf.CalculateSpace(B*P/partitions, &dont_care, &filter_unit_num_lines[i]);
    		filter_unit_size[i] = filter_unit_byte[i]*BYTE;
    		filter_unit_index[i] = tmpbf.num_probes_;
    	}
		//if ( debug ){
    		cout << "BF related: " << i << endl;
    		cout << "    bpk:         " << filter_unit_bpk[i] << endl;
    		cout << "    filter size: " << filter_unit_byte[i] << endl;
    		cout << "    indexes:     " << filter_unit_index[i] << endl;
		//}
    }

    BFHash::num_hash_indexes_ = bf_index;

    //// align the module size with data page
    //filter_unit_size = (int)round((float)(bf_size/num_filter_units)/DB_PAGE_SIZE)*DB_PAGE_SIZE;
    //filter_unit_byte = ceil(float(filter_unit_size)/BYTE);
    //filter_unit_index = bf_index/num_filter_units;

    //BFHash::num_hash_indexes_ = filter_unit_index;
    BFHash::num_filter_units_ = num_filter_units;
    BFHash::share_hash_across_levels_ = op.share_hash_across_levels;
    if(!op.share_hash_across_levels){
        BFHash::reset = false;
    }
    BFHash::share_hash_across_filter_units_ = op.share_hash_across_filter_units;
    if(op.hash_type.compare("XXHash") == 0){
       BFHash::prepareHashFuncs(XXhash);
    }else if(op.hash_type.compare("CITY") == 0){
       BFHash::prepareHashFuncs(CITY);
    }else if(op.hash_type.compare("CRC") == 0){
       BFHash::prepareHashFuncs(CRC);
    }else{
       BFHash::prepareHashFuncs(MurMur64);
    }

    debug = op.debug;

    skip = op.skip;
    skip_number = op.skip_number;
    if ( num_filter_units == 2 ){
        skip_th = {op.skip_th_1, op.skip_th_0};
    }
    else {
        skip_th.resize(num_filter_units, 0.1);
    }
    num_skip.resize(num_filter_units+1, 0);
}

int db::ReadSettings()
{
    ifstream infile;
    infile.open( settings_file );
	 
    if( infile.fail() ){
        cout << "Error opening " << settings_file << endl;
    }

    bool bf_rebuild = false;

    int num = 0;
    while( infile.peek() != EOF ){
        //int line = 0;
        string line;
        getline( infile, line);
        int value = stoi(line);
        float f_value = stof(line);
        //cout << num << " " << value << endl;
        if ( num==0 ) {
            if (value != P){
                cout << "P: " << value << endl;
                return -1;
	    }
        } else if ( num==1 ){
            if (value != B){
                cout << "B: " << value << endl;
                return -1;
	    }
        } else if ( num==2 ){
	    if (value != E){
                cout << "E: " << value << endl;
                return -1;
	    }
        } else if ( num==3 ){
            if (value != K){
                cout << "K: " << value << endl;
                return -1;
	    }
        } else if ( num==4 ){
            if (value != (int)partitioned ){
                cout << "partitioned: " << value << endl;
                return -1;
	    }
	} else if ( num==5 ){
            if (f_value != propotional){
                cout << "propotional: " << f_value << endl;
                return -1;
	    }
        } else if ( num==6 ){
            if (value != num_filter_units){
                bf_rebuild = true;
            }
        } else if ( num==7 ){
            if ( value > 0 ){
                num_levels = value;
                num_sstPerLevel.resize( num_levels, 0);
                fence_pointers.resize( num_levels );
	            bf_prime = (char*****) malloc  ( num_levels * sizeof(char****));
            }
            else {
                cout << "levels: " << value << endl;
                return -1;
            }
        } else if ( num==8 ){
            if( value == 0){
                cout << "last_sst_keys: " << value << endl;
                return -1;
            }
            last_sst_keys = value;
        } else if ( num>=9 && num<9+num_levels ){
            num_sstPerLevel[num-9] = value;
            num_sst += value;
        }
        num++;
    }
    infile.close();

    if ( num < 9+num_levels ) {
        cout << "num should be " << 8+num_levels << " " << num << endl;
        return -1;
    }
    
    if (bf_rebuild==true){
        return 1;
    }

    //cout << fence_file << endl;
    infile.open( fence_file );
    if( infile.fail() ){
        cout << "Error opening " << fence_file << endl;
    }

	//if(fastlocal_bf){
	//	FastLocalBF tmpbf = FastLocalBF(bpk/num_filter_units);
	//	filter_size = tmpbf.CalculateSpace(B*P);
	//	//last_filter_size = tmpbf.CalculateSpace(last_sst_keys);
	//	num_probes = tmpbf.num_probes_;
	//}else{
	//	LegacyBF tmpbf = LegacyBF( ceil((float)bpk/num_filter_units));
	//	uint32_t dont_care;
	//	filter_size = tmpbf.CalculateSpace(B*P/partitions, &dont_care, &num_lines);
	//	//last_filter_size = tmpbf.CalculateSpace(last_sst_keys, &dont_care, &last_num_lines);
	//	num_probes = tmpbf.num_probes_;
	//	cout << "BF related: " << endl;
	//	cout << "    bpk:         " << filter_unit_bpk[0] << endl;
	//	cout << "    filter size: " << filter_size << endl;
	//	cout << "    indexes:     " << num_probes << endl;
	//}

    for ( int i=0 ; i<num_levels ; i++ ){
        vector <string> fence_pointer_one_level ( num_sstPerLevel[i]+1 );
        bf_prime[i] = (char****) malloc (num_sstPerLevel[i]*sizeof(char***));
        for ( int j=0 ; j<=num_sstPerLevel[i] ; j++ ){
            string first, last;
            getline( infile, first );
            fence_pointer_one_level[j] = first;

            int num_keys_in_sst = ( i==num_levels-1 && j!=num_sstPerLevel[i]-1)? B*P : last_sst_keys;
            int num_partition = ( partitioned==false )? 1 : ceil(ceil(num_keys_in_sst/B)/index_in_a_page);
            int partition_size = ( partitioned==false )? num_keys_in_sst : index_in_a_page * B;

            char *** sst_partition = (char ***) malloc (num_partition*sizeof(char**));
            for( int p=0 ; p<num_partition ; p++ ){
                char** sst_bf = (char**) malloc (num_filter_units*sizeof(char*));
                for( int blo=0 ; blo<num_filter_units ; blo++ ){
                    //char* blo_bf = ( char*) malloc (filter_unit_byte*sizeof(char));
                    char* blo_bf = ( char*) malloc (filter_unit_byte[blo]*sizeof(char));
                    sst_bf[blo] = blo_bf;
                }
                sst_partition[p] = sst_bf;
            }
            bf_prime[i][j] = sst_partition;
        }
        fence_pointers[i] = fence_pointer_one_level;
    }

    return 0;
}

void db::Build( vector<vector<vector<string> > > reallocated_keys, bool bf_only )
{
	cout << "DB Build started." << endl;

	string index_command = "rm -rf " + index_dir+"/*";
	string data_command = "rm -rf " + data_dir+"/*";
	string bf_command = "rm -rf " + bf_dir+"/*";
	system(bf_command.c_str());
	if ( bf_only==false ){
		system(data_command.c_str());
		system(index_command.c_str());
	}
	
	fp_vec = vector<uint64_t> (num_levels, 0);
	n_vec = vector<uint64_t> (num_levels, 0);
	ofstream file_settings (settings_file, ios::out);
	file_settings << P << endl;
	file_settings << B << endl;
	file_settings << E << endl;
	file_settings << K << endl;
	file_settings << partitioned << endl;
	file_settings << propotional << endl;
	file_settings << num_filter_units << endl;
	hash_digests = vector<uint64_t> (num_filter_units, 0);
	file_settings << num_levels << endl;
	file_settings << last_sst_keys << endl;

	ofstream file_fence(fence_file, ios::out);
	for ( int i=0 ; i<num_levels ; i++ ){
                int len = fence_pointers[i].size();
		for ( int j=0 ; j<len ; j++ ){
			file_fence << fence_pointers[i][j] << endl;
		}
	}
	file_fence.close();
	//uint32_t dont_care;
	//if(fastlocal_bf){
	//	FastLocalBF tmpbf = FastLocalBF(bpk/num_filter_units);
	//	filter_size = tmpbf.CalculateSpace(B*P/partitions);
	//	//last_filter_size = tmpbf.CalculateSpace(last_sst_keys);
	//	num_probes = tmpbf.num_probes_;
	//}else{
	//	LegacyBF tmpbf = LegacyBF(ceil((float)bpk/num_filter_units));
	//	filter_size = tmpbf.CalculateSpace(B*P/partitions, &dont_care, &num_lines);
	//	//last_filter_size = tmpbf.CalculateSpace(last_sst_keys, &dont_care, &last_num_lines);
	//	num_probes = tmpbf.num_probes_;
	//}
	
	
	bf_prime = (char*****) malloc  ( num_levels * sizeof(char****));
	//blk_fp_prime = (char****) malloc  ( num_levels * sizeof(char***));
	blk_fp_prime.resize(num_levels);
	blk_size_prime = (int**) malloc (num_levels*sizeof(int*));
	cout << "total lv " << num_levels << endl;
	for( int l = 0; l < num_levels; l++){
		vector<vector<string> > keys_one_level = reallocated_keys[l];
		int num_sst = keys_one_level.size();
		cout << "lv " << l << " " << num_sst << endl;
		bf_prime[l] = (char****) malloc (num_sst*sizeof(char***));
		//blk_fp_prime[l] = (char***) malloc (num_sst*sizeof(char**));
		blk_size_prime[l] = (int*) malloc (num_sst*sizeof(int));
		file_settings << num_sst << endl;
		for(int sst_index = 0; sst_index < num_sst; sst_index++){
			vector<string> keys_one_sst = keys_one_level[sst_index];

			int num_partition = ( partitioned==false )? 1 : ceil(ceil(keys_one_sst.size()/B)/index_in_a_page);
			int partition_size = ( partitioned==false )? keys_one_sst.size() : index_in_a_page * B;

			char *** sst_partition = (char ***) malloc (num_partition*sizeof(char**));
			string sst_bf_prefix = bf_dir + "level_" + to_string(l) + "-sst_" + to_string(sst_index) + "-";

			for( int p=0 ; p<num_partition ; p++ ){
				char** sst_bf = (char**) malloc (num_filter_units*sizeof(char*));
				for(int blo = 0; blo < num_filter_units; blo++){
					string sst_bf_filename = sst_bf_prefix + to_string(p) + "_" + to_string(blo) +".txt";
					//cout << sst_bf_filename << endl;
					int end = (p==num_partition-1)? keys_one_sst.size() : (p+1)*partition_size;

					if(fastlocal_bf){
						//FastLocalBF bf = FastLocalBF(bpk/num_filter_units);
						int level_bpk = (monkey)? monkey_bpk[l]/num_filter_units : filter_unit_bpk[blo];
						FastLocalBF bf = FastLocalBF(level_bpk);
						for( int i=p*partition_size ; i <end ; i++){
						
							BFHash bfHash (keys_one_sst[i]);	
							vector<uint64_t>* hash_digests = bfHash.getLevelwiseHashDigest(l);
							BFHash::reset = true;
							bf.AddKey(keys_one_sst[i], hash_digests->at(blo));
						}
						bf.Finish();
						flushBFfile(sst_bf_filename, bf.data_, bf.space_);
						char* blo_bf = ( char*) malloc (bf.space_*sizeof(char));
						memcpy(blo_bf, bf.data_, bf.space_);
						sst_bf[blo] = blo_bf;
					}else{
						//LegacyBF bf = LegacyBF(bpk/num_filter_units);
						int level_bpk = (monkey)? monkey_bpk[l]/num_filter_units : filter_unit_bpk[blo];
						LegacyBF bf = LegacyBF(level_bpk);
						uint32_t temp;
                        bf.CalculateSpace(B*P/partitions, &temp, &temp);
						for( int i=p*partition_size ; i <end ; i++){
							BFHash bfHash (keys_one_sst[i]);	
							vector<uint64_t>* hash_digests = bfHash.getLevelwiseHashDigest(l);
							BFHash::reset = true;
							bf.AddKey(keys_one_sst[i], hash_digests->at(blo));
						}
						bf.Finish();
						flushBFfile(sst_bf_filename, bf.data_, bf.space_);
						char* blo_bf = ( char*) malloc (bf.space_*sizeof(char));
						memcpy(blo_bf, bf.data_, bf.space_);
						sst_bf[blo] = blo_bf;
					}
					sst_partition[p] = sst_bf;
				}

			}
			bf_prime[l][sst_index] = sst_partition;
			//cout << "BF done" << endl;

			if (bf_only==false){
				// fence pointers
				string sst_index_filename = index_dir + "level_" + to_string(l) + "-sst_" + to_string(sst_index) + ".txt";
				string sst_indexzonemap_filename = index_dir + "level_" + to_string(l) + "-sst_" + to_string(sst_index) + "_zonemap.txt";

				// building index
				//char** index_one_sst = (char**) malloc (ceil(keys_one_sst.size()/B)*sizeof(char*));
				vector<string> index_one_sst;
				vector<string> index_zonemap_vec;

				int j = 0;
				for( int i=0; i<keys_one_sst.size() ; i+=B ){
					index_one_sst.push_back(keys_one_sst[i]);
					if ( partitioned ){
						if ( i%(B*index_in_a_page)==0 ){
							index_zonemap_vec.push_back(keys_one_sst[i]);
						}
					}
				}
				//blk_fp_prime[l][sst_index] = index_one_sst;
				blk_fp_prime[l].push_back(index_one_sst);
				blk_size_prime[l][sst_index] = j;
				flushfile(sst_index_filename, &index_one_sst);
				if ( partitioned ){
					flushfile(sst_indexzonemap_filename, &index_zonemap_vec);
				}
				//cout << "Index done" << endl;
				//flushIndexes(sst_index_filename, index_one_sst, j, K);
				string sst_data_filename = data_dir + "level_" + to_string(l) + "-sst_" + to_string(sst_index) + ".txt";
				for( int i=0 ; i<keys_one_sst.size() ; i++ ){
					keys_one_sst[i].resize(E, '0');
				}
				flushfile(sst_data_filename, &keys_one_sst);
				//cout << "Data done" << endl;
			}
		}
	}

	file_settings.close();

	return;
}

string db::Get( string key, bool * result )
{
	std::chrono::time_point<std::chrono::high_resolution_clock>  total_end;
	std::chrono::time_point<std::chrono::high_resolution_clock>  total_start = high_resolution_clock::now();

	string value;
	BFHash bfHash(key);

	if ( debug ){
            cout << "lookup for the key: " << total << " " << key << endl;
	}

	int i = 0;
	while(true){
	        if ( debug ) {
			cout << "GetLevel " << i << endl;
		}
		value = GetLevel( i, bfHash, key, result );
		if ( *result == true ){
			num_lookups++;
			BFHash::reset = true;
 			total_end = high_resolution_clock::now();
			total_duration += duration_cast<microseconds>(total_end - total_start);
			return value;
		}
		i++;
		if(i >= num_levels){
			break;
		}

	}
	*result = false;
	BFHash::reset = true;

	num_lookups++;
 	total_end = high_resolution_clock::now();
	total_duration += duration_cast<microseconds>(total_end - total_start);
	
	return "";
}

inline string db::GetLevel( int i, BFHash & bfHash, string & key, bool * result )
{
    // Binary search for fense pointer
    //auto bs_start = high_resolution_clock::now();
    int bf_no = binary_search(key, fence_pointers[i]);
    //auto bs_end = high_resolution_clock::now();
    //bs_duration += duration_cast<microseconds>(bs_end - bs_start);	
    
    //auto other_start = high_resolution_clock::now();
    // no matching sst
    if(bf_no < 0){
        *result = false;
        //auto other_end = high_resolution_clock::now();
        //other_duration += duration_cast<microseconds>(other_end - other_start);
        return "";
    }

    int partition_index = 0;
    if (partitioned){
        partition_index = GetFromTopLvIndex( i, bf_no, key );
    }

    if ( debug ) {
        cout << "GetLevel " << i << " " << bf_no << " " << partition_index << endl;
    }

    //auto hash_start = high_resolution_clock::now();
    vector<uint64_t>* hash_digests = bfHash.getLevelwiseHashDigest(i);
    //auto hash_end = high_resolution_clock::now();
    //hash_duration += duration_cast<microseconds>(hash_end - hash_start);	


    // skip related
    //if ( !warmup ) {
        access_freq[i][bf_no]++;
    //}

    bool skipped = false;
    bool bf_result = QueryFilter( i, bf_no, partition_index, hash_digests, bf_prime[i][bf_no][partition_index], &skipped );

    if ( debug ) {
        cout << "QueryFilter result : " << i << " " << bf_no << " " << partition_index << " " << bf_result << endl;
    }

    if ( skipped == true ) {
        num_skip[0]++;
    }

    if ( bf_result==false ) {
		if ( !warmup ){
            total_n++;
		}
    	//n_vec[i]++;
    	*result = false;
    	//auto other_end = high_resolution_clock::now();
    	//other_duration += duration_cast<microseconds>(other_end - other_start);
    	return "";
    }

    int index_pos = GetFromIndex( i, bf_no, partition_index, key );

    if ( debug ) {
        cout << "GetFromIndex result : " << i << " " << bf_no << " " << index_pos << endl;
    }
    
    bool data_result = false;
    string data = GetFromData( i, bf_no, index_pos, key, &data_result );

    if ( debug ) {
        cout << "GetFromData result : " << i << " " << bf_no << " " << data << " " << data_result << endl;
    }

    // false positive
    if  ( !warmup ) {
        if (data_result == false){
            total_fp++;
            //fp_vec[i]++;
            total_n++;
            //n_vec[i]++;
        }else{
            total_p++;
        }
    }
	
    if (data_result == true ){
        access_tp[i][bf_no]++;
    }

    *result = data_result;
    //auto other_end = high_resolution_clock::now();
    //other_duration += duration_cast<microseconds>(other_end - other_start);
    return data;
}

float db::calcFP( int blo )
{
	float fp = 1;
	for ( int i=0 ; i<blo ; i++ ){
		//index += filter_unit_index[blo];
		//bpk   += filter_unit_bpk[blo];
	    fp = fp * pow(1-exp((-1)*(float)filter_unit_index[i]/(filter_unit_bpk[i])), filter_unit_index[i]);
	}

    return fp;
}

inline bool db::QueryFilter( int i, int bf_no, int partition, vector<uint64_t>* hash_digests, char** bf_list, bool * skipped )
{
        if ( debug ) {
            cout << "QueryFilter started " << endl;
        }
    bool result = true;

    //float fp[11] = {1, 1, 0.393, 0.237, 0.147, 0.092, 0.0561, 0.0347, 0.0216, 0.0133, 0.00819};
    float target_fp = 0.00819;

    float alpha = float(access_tp[i][bf_no])/access_freq[i][bf_no];
    float beta  = float(access_freq[i][bf_no])/(total+1);


/*	
    if(i == num_levels - 1 && bf_no == num_sstPerLevel[i]-1){
        tmp_filter_size = last_filter_size;
    }
    if(i == num_levels - 1 && bf_no == num_sstPerLevel[i]-1){
        tmp_num_lines = last_num_lines;
    }
*/

    for(int blo = 0; blo < num_filter_units; blo++){
		int level_bpk = (monkey)? monkey_bpk[i]/num_filter_units : filter_unit_bpk[blo];
		if ( monkey ){
			LegacyBF bf = LegacyBF(level_bpk);
			uint32_t temp;
    		filter_unit_byte[blo] = bf.CalculateSpace(B*P/partitions, &temp, &filter_unit_num_lines[blo]);
    		filter_unit_size[blo] = filter_unit_byte[blo]*BYTE;
    		filter_unit_index[blo] = bf.num_probes_;
		}

    	int filter_size = filter_unit_byte[blo];
    	int num_lines = filter_unit_num_lines[blo];

        char * filter = (char*) malloc (filter_size*sizeof(char));
        string sst_bf_prefix = bf_dir + "level_" + to_string(i) + "-sst_" + to_string(bf_no) + "-";
        string sst_bf_filename = sst_bf_prefix + to_string(partition) + "_" + to_string(blo) +".txt";
    
        if ( debug ) {
            cout << "QueryFilter: " << i << " " << bf_no << " " << partition << " " << blo << endl;
        }

        // skip
        if ( !warmup && skip && blo>=(num_filter_units-skip_number) ){
            float exp_io  = beta * ( alpha + (1-alpha)*calcFP(blo+1) );
            float base_io = beta * ( alpha + (1-alpha)*calcFP(blo) );
            //float exp_io  = beta * ( alpha + (1-alpha)*pow(fp, blo+1) );
            //float base_io = beta * ( alpha + (1-alpha)*pow(fp, blo) );
            //float can_exp_io  = ( alpha + (1-alpha)*pow(fp, blo+1) );
            //float can_base_io = ( alpha + (1-alpha)*pow(fp, blo) );
            float utility = base_io - exp_io;

            //if ( skip ){
            if ( debug ){
                std::cout << std::fixed;
                std::cout << std::setprecision(5);
                cout << "skip: " << blo << "\t" << alpha << "\t" << beta << "\t" << calcFP(blo+1) << "\t" << calcFP(blo) << "\t" << utility << endl;
                //cout << "skip: " << blo << "\t" << i << "\t" << bf_no << "\t" << alpha << "\t" << beta << "\t" << endl;
            }
            //if ( exp_io >= skip_th[blo] ){
            if ( utility <= skip_th[blo] ){
                num_skip[num_filter_units-blo]++;
                *skipped = true;
                return true;
            }
        }
        if ( !warmup ){
            qnum++;
            (qnum_histogram->at(blo))++;
        }

        if ( block_cache_en==true ){
            string module_str;
            vector<string> block;
            block = block_cache->refer(sst_bf_filename, filter_pri);

            // cache miss
            if (block.size() == 0){
                if ( debug ) {
                    cout << "QueryFilter cache miss: " << i << " " << bf_no << " " << partition << " " << blo << endl;
                    cout << "QueryFilter BF file read: " <<  sst_bf_filename << endl;
                }
                if (warmup==false){
                    //readbytes += filter_unit_byte;
                    readbytes += filter_unit_byte[blo];
                    total_io  += (int)round((float)filter_unit_byte[blo]/DB_PAGE_SIZE);
                    bloom_miss++;
                }
                read_bf(sst_bf_filename, filter, filter_size);
                if ( debug ) {
                    cout << "QueryFilter BF file read done " << endl;
                }
                //module_str.assign(filter, filter + filter_unit_byte);
                module_str.assign(filter, filter + filter_unit_byte[blo]);
                vector <string> to_cache(1, module_str);
                block_cache->add( sst_bf_filename, filter_pri, to_cache );
                if ( debug ) {
                    cout << "QueryFilter BF added to Block Cache" << endl;
                }
            }
            // cache hit
            else{
                if ( debug ) {
                    cout << "QueryFilter cache hit: " << i << " " << bf_no << " " << partition << " " << blo << endl;
                }
                if (warmup==false){
                    bloom_hit++;
                }
                filter = (char *)block.at(0).c_str();
            }
        }
        else {
            filter = bf_list[blo];
        }
        //if ( debug ) {
        //    cout << "QueryFilter BF querying: " << endl;
        //    for (std::size_t i = 0; i < filter_unit_byte[blo] ; ++i)
        //    {
        //        cout << bitset<8>(filter[i]) << endl;
        //    }
        //}

        if(fastlocal_bf){
            if(!FastLocalBF::MayMatch(hash_digests->at(blo), filter_size, filter_unit_index[blo], filter)) return false;
        }else{
            if(!LegacyBF::MayMatch(hash_digests->at(blo), num_lines, filter_unit_index[blo], filter)) return false;
        }
    }

    return result;
}


int db::GetFromTopLvIndex( int l, int bf_no, string key )
{
	int index_pos = 0;
	vector <string> index;
	vector<string> filter;

	string sst_index_filename = index_dir + "level_" + to_string(l) + "-sst_" + to_string(bf_no) + "_zonemap.txt";


	int local_index_size = (partitioned)? index.size() : index_size;
	if ( block_cache_en ){
		filter = block_cache->refer(sst_index_filename, index_pri);
		if (filter.size() == 0){
			read_index( sst_index_filename, K, 0, false, index);
			if (warmup==false) {
				//readbytes += (partitioned)? index.size() : index_size;
				readbytes += local_index_size;
				total_io  += (int)ceil( (float)local_index_size/DB_PAGE_SIZE );
				index_miss++;
			}
			block_cache->add(sst_index_filename, index_pri, index );
		}
		else{
			index = filter;
			if (warmup==false) index_hit++;
		}
	}
	else {
		read_index( sst_index_filename, K, 0, false, index);
		readbytes += (partitioned)? index.size() : index_size;
	}
	index_pos = binary_search( key, index );

	return index_pos;
}


inline int db::GetFromIndex( int l, int bf_no, int partition, string key )
{
	vector<string> index;
	int data_in_partition = P / (partitions);

	if ( block_cache_en ){
		vector<string> block;
		string sst_index_filename = index_dir + "level_" + to_string(l) + "-sst_" + to_string(bf_no) + ".txt";

		block = block_cache->refer(sst_index_filename, index_pri);
		if (block.size() == 0){
			if (warmup==false) {
				readbytes += index_size;
				total_io  += (int)ceil( (float)index_size/DB_PAGE_SIZE );
				index_miss++;
			}
			read_index( sst_index_filename, K, partition, partitioned, index);
                        //read_index( string filename, int size, char** index)
			block_cache->add(sst_index_filename, index_pri, index );
		}
		else{
			index = block;
			if (warmup==false) index_hit++;
		}
	}
	else {
		index = blk_fp_prime[l][bf_no];
	}

	//int index_pos = binary_search(key, index, blk_size_prime[l][bf_no]);
	//cout << key << " " << l << " " << index_pos << endl;
	return (data_in_partition*partition + binary_search(key, index));
}

inline string db::GetFromData( int i, int bf_no, int index_pos, string key, bool * result )
{
	vector <string> data_block;
	vector <string> block;
	string sst_data_filename = data_dir + "level_" + to_string(i) + "-sst_" + to_string(bf_no) + ".txt";

	if ( block_cache_en ){
		block = block_cache->refer(sst_data_filename+to_string(index_pos), data_pri);
		if (block.size() == 0){
			if (warmup==false) {
				readbytes += data_size;
                total_io  += (int)ceil((float)B*E/DB_PAGE_SIZE);
				data_miss++;
			}
			read_data( sst_data_filename, index_pos, key.size(), data_block );
			block_cache->add( sst_data_filename+to_string(index_pos), data_pri, data_block );
		}
		else {
			data_block = block;
			if (warmup==false) data_hit++;
		}
	}
	else {
		read_data( sst_data_filename, index_pos, key.size(), data_block );
	}
	
	int found = -1;
	for ( int k=0 ; k<data_block.size() ; k++){
		string data_entry = data_block[k].substr( 0, key.size() );
		if(data_entry.compare(key) == 0){
			found = k;
		}
	}
	if ( found == -1 ){
		*result = false;
		return "";
	}
	else {
		*result = true;
		return key; 
	}
}

inline int db::read_data ( string filename, int pos, int key_size, vector<string> & data_block )
{
	//int flags = O_RDWR | O_DIRECT | O_SYNC;
	mode_t mode=S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;//644

	int fd = open(filename.c_str(), file_read_flags, mode );
	if (fd <= 0) {
    	printf("Error %s\n", strerror(errno));
		cout << "Cannot open partiton file " << filename << endl;
		return 0;
	}
      
	data_block.clear();
	data_block.resize(B, "");

	char* buf;
	int offset = pos*B*E;


	auto data_start = high_resolution_clock::now();
	posix_memalign((void**)&buf,getpagesize(),DB_PAGE_SIZE);
	memset(buf, 0, DB_PAGE_SIZE);


	//read( fd, buf, DB_PAGE_SIZE );
	//auto data_end   = high_resolution_clock::now();
	//data_duration += duration_cast<microseconds>(data_end - data_start);

      
    // Moving pointer
	lseek( fd, DB_PAGE_SIZE+offset, SEEK_SET );

	memset(buf, 0, DB_PAGE_SIZE);
	//data_start = high_resolution_clock::now();
	read( fd, buf, DB_PAGE_SIZE );
	auto data_end   = high_resolution_clock::now();
	data_duration += duration_cast<microseconds>(data_end - data_start);

	//int key_size;
	int entry_size = E;
	char* tmp_buf = (char*) malloc(entry_size+1);
	offset = 0;
	tmp_buf[entry_size] = '\0';
	for(int i = 0; i < B; i++){
		if(offset + key_size >= DB_PAGE_SIZE){
			unsigned int inner_key_offset = 0;
			if(offset < DB_PAGE_SIZE){
				memcpy(tmp_buf, buf+offset,DB_PAGE_SIZE-offset);
				inner_key_offset = DB_PAGE_SIZE-offset;
			}
		
			 data_start = high_resolution_clock::now();
			read( fd, buf, DB_PAGE_SIZE );
			data_end   = high_resolution_clock::now();
			data_duration += duration_cast<microseconds>(data_end - data_start);

			offset = key_size - inner_key_offset;	
			memcpy(tmp_buf+inner_key_offset, buf, offset);
		}else{

			memcpy(tmp_buf, buf+offset, E);
			offset += E;
		}

		data_block[i] = string(tmp_buf);

		memset(tmp_buf, 0, key_size);
	}
	free(tmp_buf);

	free( buf );
	close( fd );

        if(delay > 0){
			 data_start = high_resolution_clock::now();
	    struct timespec req = {0}; 
            req.tv_sec = 0;
	    req.tv_nsec = delay;
	    nanosleep(&req, (struct timespec *)NULL);
			data_end   = high_resolution_clock::now();
			data_duration += duration_cast<microseconds>(data_end - data_start);
	}
		
	return 0;
}

inline int db::binary_search(string key, char** indexes, int size)
{
	int len = size;
	if(len == 0){return -1;}
	if(len == 1){return 0;}
	int start = 0;
	int end = len - 1;
	int mid;

	//for (int i=0 ; i<len ; i++){
	//	cout << fence_pointer[i] << endl;;
	//}

	if(strcmp(key.c_str(), indexes[start]) < 0){ return -1;}
	if(strcmp(key.c_str(), indexes[end]) > 0){ return -1;}

	while(end - start > 1){
		mid = (start + end)/2;
		if(strcmp(key.c_str(), indexes[mid])< 0){
			end = mid;
		} 
		else if(strcmp(key.c_str(), indexes[mid])  == 0){
			return mid;
		}
		else{
			start = mid;
		}
	}
	return start;
}



int db::binary_search(string key, vector<string> & fence_pointer)
{
	int len = fence_pointer.size();
	if(len == 0){return -1;}
	if(len == 1){return 0;}
	int start = 0;
	int end = len - 1;
	int mid;

	//for (int i=0 ; i<len ; i++){
	//	cout << fence_pointer[i] << endl;;
	//}

	if(key.compare(fence_pointer[start]) < 0){ return -1;}
	if(key.compare(fence_pointer[end]) > 0){ return -1;}

	while(end - start > 1){
		mid = (start + end)/2;
		if(key.compare(fence_pointer[mid]) < 0){
			end = mid;
		} 
		else if(key.compare(fence_pointer[mid]) == 0){
			return mid;
		}
		else{
			start = mid;
		}
	}
	return start;
}

void db::split_keys( vector<string> table_in, vector<vector<vector<string> > > & reallocated_keys )
{
	// calculate how many keys assigned in each level
	int num_keys = table_in.size();
	int level = 1;
	vector<int> bf_num_keys;
	if(num_keys < buffer_size){
		bf_num_keys.push_back(num_keys);	
	}else{
		num_sst = ceil((float)num_keys/(float)buffer_size);
		int tmp_num_keys = num_keys;
		level = (int) ceil(log(num_keys*1.0*(size_ratio - 1)/buffer_size + 1)/log(size_ratio));
		bf_num_keys = vector<int> (level, 0);
		// build the tree bottom-to-up
		for(int i = level - 1; i >= 1; i--){
			int sst_previous_levels = pow(size_ratio, i) - 1;
			//int sst_previous_levels = pow(size_ratio, i)/(size_ratio-1);
			int capacity_previous_levels = buffer_size * sst_previous_levels;
			if(capacity_previous_levels > tmp_num_keys) continue;
			int num_keys_curr_level = tmp_num_keys - capacity_previous_levels;
			bf_num_keys[i] = num_keys_curr_level;
			tmp_num_keys -= num_keys_curr_level;
		}
		bf_num_keys[0] = tmp_num_keys;
		
		int sum_keys = 0;
		for(int i = 0; i < level; i++){
			sum_keys += bf_num_keys[i];
		}
	}
	num_levels = level;
	num_sstPerLevel.resize( num_levels, 0);

	// allocate keys in each level
	vector<vector<string> > reversed_leveled_keys; // store the keys in each level reversely
	int start_index = 0;
	for(int i = level - 1; i >= 0; i--){
		int end_index = bf_num_keys[i] + start_index;
		if(end_index >= num_keys){
			reversed_leveled_keys.push_back(vector<string> (table_in.begin() + start_index, table_in.end()));
		}else{
			reversed_leveled_keys.push_back(vector<string> (table_in.begin() + start_index, table_in.begin() + end_index));
		}
		start_index = end_index;
	}	
	// split keys into equal-size sstables
	reallocated_keys.clear();
	fence_pointers.clear();
	for(int i = level - 1; i >= 0; i--){
		vector<vector<string> > sst_keys_one_level;
		vector<string> fence_pointer_one_level;
		split_keys_sst(buffer_size, reversed_leveled_keys[i], sst_keys_one_level, fence_pointer_one_level);

		num_sstPerLevel[(level-1)-i] = sst_keys_one_level.size();
		reallocated_keys.push_back(sst_keys_one_level);
		fence_pointers.push_back(fence_pointer_one_level);
		if(i == level - 1){
			last_sst_keys = sst_keys_one_level.back().size();
		}
	}
	SetTreeParam();
}


void db::SetTreeParam()
{
	positive_counters.resize(num_levels, 0);
	qnum_histogram = new vector<int> (num_filter_units, 0); 
	lnum_histogram = new vector<int> (num_levels, 0); 

	access_freq.resize(num_levels);
	access_tp.resize(num_levels);

	for( int l = 0; l < num_levels; l++){
		int num_l_sst = num_sstPerLevel[l];
		access_freq[l] = vector<int>(num_l_sst, 0);
		access_tp[l] = vector<int>(num_l_sst, 0);
		//cout << "set param " << num_levels << " " << num_l_sst << endl;
	}

	bf_tp_eval_histogram = new vector<int> (num_levels, 0);
}


void db::split_keys_sst(int sst_capacity, vector<string> keys_one_level, vector<vector<string> > & sst, vector<string> & fence_pointer )
{
	sst.clear();
	fence_pointer.clear();
	if(keys_one_level.size() == 0) return;
	sort(keys_one_level.begin(), keys_one_level.end());
	int start_index = 0;
	int num_keys_one_level = keys_one_level.size();
	while(true){
		int end_index = start_index + sst_capacity;
		fence_pointer.push_back(keys_one_level[start_index]);
		if(end_index >= num_keys_one_level){
			sst.push_back(vector<string> (keys_one_level.cbegin() + start_index, keys_one_level.cend()));
			break;
		}else{
			sst.push_back(vector<string> (keys_one_level.cbegin() + start_index, keys_one_level.cbegin() + end_index));
		}
		start_index = end_index;
	}	
	fence_pointer.push_back(keys_one_level.back());	
}

void db::flushBFfile(string filename, const char* sst_bf_p, uint32_t size){
	ofstream outfile(filename, ios::out);
	char buffer[sizeof(unsigned int)];
	memset(buffer, 0, sizeof(unsigned int));
	
	for(int i = 0; i < size; i++){
		memcpy(buffer, &(sst_bf_p[i]), sizeof(char));
		outfile.write(buffer, sizeof(char));
		memset(buffer, 0, sizeof(char));
	}	
	outfile.close();

}
void db::flushIndexes( string filename, char** indexes, int size, int key_size){
	ofstream outfile(filename, ios::out);
	char buffer[DB_PAGE_SIZE];

	memcpy(buffer, &size, sizeof(unsigned int));
	outfile.write(buffer, sizeof(unsigned int));
	memset(buffer, 0, sizeof(unsigned int));

	memcpy(buffer, &key_size, sizeof(unsigned int));
	outfile.write(buffer, sizeof(unsigned int));
	memset(buffer, 0, sizeof(unsigned int));

	memset(buffer, 0, DB_PAGE_SIZE-2*sizeof(unsigned int));
	outfile.write(buffer, DB_PAGE_SIZE-2*sizeof(unsigned int));

	for(int i = 0 ;i < size; i++){
		memcpy(buffer, indexes[i], key_size);
		outfile.write(buffer, key_size);
		memset(buffer, 0, key_size);
	}
}
void db::flushfile( string filename, vector<string>* table){
	ofstream outfile(filename, ios::out);
	char buffer[DB_PAGE_SIZE];

	int size = table->size();
	memcpy(buffer, &size, sizeof(unsigned int));
	outfile.write(buffer, sizeof(unsigned int));
	memset(buffer, 0, sizeof(unsigned int));

	int key_size = (table->at(0)).size();
	memcpy(buffer, &key_size, sizeof(unsigned int));
	outfile.write(buffer, sizeof(unsigned int));
	memset(buffer, 0, sizeof(unsigned int));

	memset(buffer, 0, DB_PAGE_SIZE-2*sizeof(unsigned int));
	outfile.write(buffer, DB_PAGE_SIZE-2*sizeof(unsigned int));

	for(int i = 0 ;i < size; i++){
		char* str = (char*) malloc(key_size);
		strcpy (str, table->at(i).c_str());
		memcpy(buffer, str, key_size);
		outfile.write(buffer, key_size);
		memset(buffer, 0, key_size);
	}
}


void db::PrintStat(double program_total, double data_load_total)
{
	// log file
	string out_path_cmd = "mkdir -p " + out_path;
	system(out_path_cmd.c_str());
	string file_result = out_path + "result.txt";
	ofstream result_file(file_result);
	result_file << "Total positives:\t" << total_p << endl;
	result_file << "Total false positives:\t" << total_fp << endl;
	result_file << "Total negatives:\t" << total_n << endl;
	result_file << "FPR:\t" << (double) total_fp/total_n << endl;

	double total = total_duration.count()/tries;
	result_file << "Total query time:\t" << total  << endl;
	total -= data_duration.count()/tries;
	result_file << "Data access time:\t" << data_duration.count()/tries << endl;
	result_file << "Program total time:\t" << program_total/tries << endl;
	result_file << "Data input time:\t" << data_load_total/tries << endl;
	total -= hash_duration.count()/tries;
	result_file << "Hash time:\t" << hash_duration.count()/tries << endl;
	result_file << "Other time:\t" << total << endl;
	
	result_file << endl;

	result_file << "bf qnum: " << qnum/tries << endl;
	result_file << "bf miss: " << bloom_miss/tries << endl;
	//int filter_page = (int)ceil((float)filter_unit_byte/DB_PAGE_SIZE);
	result_file << "bf hit:  " << bloom_hit/tries << endl;
	//result_file << "bf I/O:  " << (bloom_miss*filter_page)/tries << endl;
	result_file << "index miss: " << index_miss/tries << endl;
	int index_page = (int)ceil((float)K*P/DB_PAGE_SIZE);
	result_file << "index hit:  " << index_hit/tries << endl;
	result_file << "index I/O:  " << (index_miss*index_page)/tries << endl;
	result_file << "data miss: " << data_miss/tries << endl;
	result_file << "data I/O:  " << (data_miss*E*B/DB_PAGE_SIZE)/tries << endl;
	result_file << "data hit:  " << data_hit/tries << endl;
	result_file << "total_io:  " << total_io/tries << endl;
	result_file << endl;

	if ( skip ) {
		result_file << "number of skip:  " << num_skip[0] << endl;
		for( int i=1 ; i<num_filter_units+1 ; i++ ) {
			result_file << num_skip[i] << "\t";
		}
		result_file << endl;
		result_file << endl;
	}

	for( int i=0 ; i<num_filter_units; i++ ){
		result_file << "qnum " << i << " : " << qnum_histogram->at(i) << endl;
		cout << "qnum " << i << " : " << qnum_histogram->at(i) << endl;
	}

	result_file.close();

	return;
}

inline void db::read_bf(string filename, char* bf_buffer, int size){
	//int flags = O_RDWR | O_DIRECT;
	int flags = O_RDWR;
	mode_t mode=S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;//644

	
	int fd_ = open(filename.c_str(), flags, mode );
	if (fd_ <= 0) {
		cout << "Cannot open partiton file " << filename << endl;
    	printf("Error %s\n", strerror(errno));
	}
	
	char* buffer_;	
	posix_memalign((void**)&buffer_,DB_PAGE_SIZE,DB_PAGE_SIZE);
	memset(buffer_, 0, DB_PAGE_SIZE);

	for(int i = 0; i < size; i++){
		if ( i%DB_PAGE_SIZE == 0){
			memset(buffer_, 0, DB_PAGE_SIZE);
			read( fd_, buffer_, DB_PAGE_SIZE );
		}
		memcpy(bf_buffer+i, buffer_+(i%DB_PAGE_SIZE), sizeof(char));
	}
    //cout << "filter size:" << filter_unit_byte << endl;
    //for (std::size_t i = 0; i < filter_unit_byte ; ++i)
    //{
    //    cout << bitset<8>(bf_buffer[i]) << endl;
    //}

	if(fd_ > 0)
		close( fd_ );	
}

inline int db::read_index_size( string filename ){
	//int flags = O_RDWR | O_DIRECT | O_SYNC;
	int flags = O_RDWR;
	mode_t mode=S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;//644

	int fd = open(filename.c_str(), flags, mode );
	if (fd <= 0) {
		cout << "Cannot open partiton file " << filename << endl;
    	printf("Error %s\n", strerror(errno));
		return 0;
	}

	char* buf;
	posix_memalign((void**)&buf,getpagesize(),DB_PAGE_SIZE);

	memset(buf, 0, DB_PAGE_SIZE);
	read( fd, buf, DB_PAGE_SIZE );
	
	int size;
	int key_size;
	memcpy(&size, buf, sizeof(unsigned int));	
	index_size = size;
	memcpy(&key_size, buf+sizeof(unsigned int), sizeof(unsigned int));	
	free( buf );
	if(fd > 0)
		close( fd );	
	return size;
}

inline void db::read_index( string filename, int size, char** index)
{
	//int flags = O_RDWR | O_DIRECT | O_SYNC;
	int flags = O_RDWR | O_SYNC;
	mode_t mode=S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;//644

	int fd = open(filename.c_str(), flags, mode );
	if (fd <= 0) {
		cout << "Cannot open partiton file " << filename << endl;
    	printf("Error %s\n", strerror(errno));
		return;
	}

	char* buf;
	posix_memalign((void**)&buf,getpagesize(),DB_PAGE_SIZE);

	memset(buf, 0, DB_PAGE_SIZE);
	read( fd, buf, DB_PAGE_SIZE );
	
	int key_size;
	memcpy(&key_size, buf+sizeof(unsigned int), sizeof(unsigned int));	

	int offset = 0;

	memset(buf, 0, DB_PAGE_SIZE);
	read( fd, buf, DB_PAGE_SIZE );

	for(int i = 0; i < size; i++){
		if(offset + key_size >= DB_PAGE_SIZE){
			unsigned int inner_key_offset = 0;
			if(offset < DB_PAGE_SIZE){
				memcpy(index[i], buf+offset,DB_PAGE_SIZE-offset);
				inner_key_offset = DB_PAGE_SIZE-offset;
			}
		
			read( fd, buf, DB_PAGE_SIZE );

			offset = key_size - inner_key_offset;	
			memcpy(index[i]+inner_key_offset, buf, offset);
		}else{
			memcpy(index[i], buf+offset, key_size);
			offset += key_size;
		}
		index[i][key_size] = '\0';
		
	}
	memset(index[size], 'z', key_size);
	index[size][key_size] = '\0';

	free( buf );
	close( fd );
	return;
}

inline void db::read_index( string filename, int size, vector<string> &index)
{
	int flags = O_RDWR | O_SYNC;
	mode_t mode=S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;//644

	int fd = open(filename.c_str(), flags, mode );
	if (fd <= 0) {
		cout << "Cannot open partiton file " << filename << endl;
    	printf("Error %s\n", strerror(errno));
		return;
	}

	char* buf;
	posix_memalign((void**)&buf,getpagesize(),DB_PAGE_SIZE);

	memset(buf, 0, DB_PAGE_SIZE);
	read( fd, buf, DB_PAGE_SIZE );
	
	int key_size;
	memcpy(&size, buf, sizeof(unsigned int));	
	memcpy(&key_size, buf+sizeof(unsigned int), sizeof(unsigned int));	

	char* tmp_buf = (char*) malloc(key_size+1);
	tmp_buf[key_size] = '\0';
	int offset = 0;

	memset(buf, 0, DB_PAGE_SIZE);
	read( fd, buf, DB_PAGE_SIZE );

	for(int i = 0; i < size; i++){
		if(offset + key_size >= DB_PAGE_SIZE){
			unsigned int inner_key_offset = 0;
			if(offset < DB_PAGE_SIZE){
				memcpy(tmp_buf, buf+offset,DB_PAGE_SIZE-offset);
				inner_key_offset = DB_PAGE_SIZE-offset;
			}
		
			read( fd, buf, DB_PAGE_SIZE );

			offset = key_size - inner_key_offset;	
			memcpy(tmp_buf+inner_key_offset, buf, offset);
		}else{
			memcpy(tmp_buf, buf+offset, key_size);
			offset += key_size;
		}
		index[i] = string(tmp_buf);
		memset(tmp_buf, 0, key_size);
	}
	index[size] = std::string(key_size, 'z');

	free(tmp_buf);
	free( buf );
	close( fd );
	return;
}

void db::read_index( string filename, int key_size, int index_pos, bool partial, vector<string> & index )
{
	int flags = O_RDWR | O_DIRECT | O_SYNC;
	mode_t mode=S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;//644

	int fd = open(filename.c_str(), flags, mode );
	if (fd <= 0) {
		cout << "Cannot open partiton file " << filename << endl;
    	printf("Error %s\n", strerror(errno));
		return;
	}

	char* buf;
	posix_memalign((void**)&buf,getpagesize(),DB_PAGE_SIZE);

	memset(buf, 0, DB_PAGE_SIZE);
	read( fd, buf, DB_PAGE_SIZE );
	
	int size;
	int key_size_from_file;
	memcpy(&size, buf, sizeof(unsigned int));	
	memcpy(&key_size_from_file, buf+sizeof(unsigned int), sizeof(unsigned int));	

	int offset = ( partial )? (DB_PAGE_SIZE)*index_pos : 0;
    // Moving pointer
	lseek( fd, DB_PAGE_SIZE+offset, SEEK_SET );

	char* tmp_buf = (char*) malloc(key_size+1);
	offset = 0;
	tmp_buf[key_size] = '\0';

	memset(buf, 0, DB_PAGE_SIZE);
	read( fd, buf, DB_PAGE_SIZE );

	size = (partial)? DB_PAGE_SIZE/key_size : size;
	index.clear();
	index.resize(size+1, "");
	for(int i = 0; i < size; i++){
		if(offset + key_size >= DB_PAGE_SIZE){
			unsigned int inner_key_offset = 0;
			if(offset < DB_PAGE_SIZE){
				memcpy(tmp_buf, buf+offset,DB_PAGE_SIZE-offset);
				inner_key_offset = DB_PAGE_SIZE-offset;
			}
		
			read( fd, buf, DB_PAGE_SIZE );

			offset = key_size - inner_key_offset;	
			memcpy(tmp_buf+inner_key_offset, buf, offset);
		}else{
			memcpy(tmp_buf, buf+offset, key_size);
			offset += key_size;
		}

		index[i] = string(tmp_buf);
		memset(tmp_buf, 0, key_size);
	}

	index[size] = std::string(key_size, 'z');
	free(tmp_buf);

	free( buf );
	close( fd );
}

void db::SetBlockCache( options op )
{
	block_cache_en = op.block_cache;
	block_cache_size = op.block_cache_size;
	block_cache_priority = op.block_cache_priority;
	//cout << "block cache " << block_cache_size << endl;

	if (block_cache_en){
		block_cache = new PriorityCache( block_cache_size, block_cache_priority );
	}
}
