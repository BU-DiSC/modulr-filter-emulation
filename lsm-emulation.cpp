using namespace std;

#include "stdafx.h"
#include "BF_bit.h"
#include "hash/md5.h"

#include "options.h"
#include "db.h"

#include "time.h"
#include "math.h"

#include <bits/stdc++.h> 
#include <algorithm>
#include <fcntl.h>
#include <unistd.h>
#include <linux/fs.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <utility>

#define DB_PAGE_SIZE 4096

fsec load_duration = std::chrono::microseconds::zero();

void loadfile( string filename, vector<string>* table, int* num );

int main(int argc, char * argv[])
{
	auto t_start = high_resolution_clock::now();
	options op;
	// parse the command line arguments
	if ( op.parse(argc, argv) ) {
	  exit(1);
	}
	string filename = op.out_path;
	cout << filename << endl;

	int aloc = 0;

	// -- load workload --------------------------------------------------------------------
	string file_in_set  = op.insert_path;
	string file_out_set = op.query_path;

	vector<string> table_in;
	vector<string> table_out;

	int S_size = 0;
	int SC_size = 0;
	auto l_start = high_resolution_clock::now();
	loadfile( file_out_set, &table_out, &SC_size );
	auto l_end = high_resolution_clock::now();
	load_duration += duration_cast<microseconds>(l_end - l_start);

	op.key_size = table_out.at(0).size();
	// -------------------------------------------------------------------------------------
	
	vector<vector<vector<string> > > reallocated_keys;

        db database( op );

	// -- Program && Bloom Filters -------------------------------------------------------------------
	// log file
	bool bf_only = false;
	bool rebuild = false;
	cout << "dd " << op.destroy_db << endl;
	cout << "path " << op.db_path << endl;
	if ( op.destroy_db==false ){
		int read_result  = database.ReadSettings();
		cout << "read db check " << read_result << endl;
		bf_only = ( read_result==1 )? true : false;
		rebuild = ( read_result==-1 )? true : false;
		cout << bf_only << " " << rebuild << endl;
	}
	if ( op.destroy_db || rebuild || bf_only ) {
		loadfile( file_in_set, &table_in, &S_size );
		database.split_keys( table_in, reallocated_keys );
		database.Build( reallocated_keys, bf_only );
	}
	else {
		database.SetTreeParam();
		//load_duration += database.loadBFAndIndex();
	}
	database.SetBlockCache( op );
	cout << "DB has been loaded" << endl; 
	// ------------------------------------------------------------------------------


	// -- Query ---------------------------------------------------------------------
	unsigned long total_query = table_out.size();

	int temp = 0;

	string input_str;
	srand(time(0));

	bool result;

	for(int k = 0; k < database.tries; k++){
		cout << "Run " << k << " : " << endl;
		database.total  = 0;
		// warmup
		if ( op.warmup ) {
			database.warmup = true;
			for ( long i=0 ; i<table_out.size() ; i+=10 ) {
				input_str = table_out[i];
				result = false;
				database.Get( input_str, &result );

				input_str.clear();
				database.total++;
			}
		}
	
        cout << "warmup done" << endl;
		database.warmup = false;
		for ( long i=0 ; i<table_out.size() ; i++ ) {
			input_str = table_out[i];
			//cout << input_str << endl;
			result = false;
			database.Get( input_str, &result );

			input_str.clear();
			database.total++;
			//if ( !op.debug ) {
			//	if ( i%(table_out.size()/10) == 0 ) cout << i/(table_out.size()/10)*10 << "%" << std::flush;
			//	if ( i%(table_out.size()/100) == 0 && i != 0 ) cout << "=" << std::flush;
			//}
		}
	}
	
	// ------------------------------------------------------------------------------
	// log file
	cout << "Done " << endl;
	auto t_end = high_resolution_clock::now();
	fsec t_duration = duration_cast<std::chrono::microseconds>(t_end-t_start);
	database.PrintStat(t_duration.count(), load_duration.count());
	return temp;
}

void loadfile( string filename, vector<string>* table, int* num )
{
	ifstream infile;
	infile.open( filename );

	if( infile.fail() ){
		cout << "Error opening " << filename << endl;
	}

	while( infile.peek() != EOF ){
		string line;
		getline(infile,line);
		int pos1 = line.find(" ");
		int pos2 = line.find(" ", pos1+2);
	    string data = line.substr(pos1+1, pos2-pos1-1);
		//cout << data << " " << data.size() << endl;
		table->push_back( data );
		(*num)++;
	}

	infile.close();
	return;
}
