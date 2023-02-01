using namespace std; 

#include <bits/stdc++.h> 
#include "LRUcache.h"
#include "PriorityCache.h"

// Declare the size 
PriorityCache::PriorityCache(int n) 
{ 
    capacity = n; 

    pri_num = 1;
    size_ratio = (int*)malloc(pri_num*sizeof(int));
    *size_ratio = n/pri_num;

    LRUCache tmp(n/pri_num);

    cache.resize(pri_num, tmp);
} 

// Declare the size & priority
PriorityCache::PriorityCache(int n, int pri) 
{ 
    capacity = n; 

    pri_num = pri;
    size_ratio = (int*)malloc(pri_num*sizeof(int));

    for ( int i=0 ; i<pri_num ; i++ ){
        size_ratio[i] = (i+1)*(n/pri_num);
    }

    LRUCache tmp(n/pri_num);

    cache.resize(pri_num, tmp);
} 

// add a block
void PriorityCache::add(string x, int pri, vector <string> content) 
{
    int size = content.size() * content.at(0).size();
    if ( capacity < size ){
        cout << "error: content is larger than cache size" << endl;
        return;
    }

    for( int i=0 ; i<pri ; i++ ){
        if( cache[i].get_remain() >= size){
            cache[pri].add( x, content);
            return;
        }
    }
    //cout << x << " " << pri << endl;
    if( pri==pri_num-1 ){
        //cout << x << " " << pri << " " << cache[pri].get_remain() << endl;
        cache[pri].add(x, content);
        return;
    }

    if( cache[pri].get_remain() >= size){
        cache[pri].add( x, content);
    }
    else{
        //cout << x << " " << pri << " " << cache[pri].get_remain() << endl;
        while ( cache[pri].get_remain() < size){
            string pop_name;
            vector <string> pop_content;
            pop_content = cache[pri].pop( pop_name );
            //cout << "pop " << pop_name <<endl;
            add( pop_name, pri+1, pop_content );
        }
        cache[pri].add( x, content );
    }

    return;
}

vector <string> PriorityCache::refer( string input_str, int pri )
{ 
    vector <string> output;
    for( int i=pri ; i<pri_num ; i++ ){
        //LRUCache currentCache = cache[i];
        output = cache[i].refer( input_str );
        if ( output.size()!=0 ){
            if ( i < pri ){
                cache[i].append( input_str, output );
            }
            else if ( i!= pri ){
                string pop_name;
                vector <string> pop_content;
                pop_content = cache[i].head( pop_name );
                add( pop_name, pri, pop_content );
            }
            return output;
        }
    }
    return output;
} 

// Function to display contents of cache 
void PriorityCache::display() 
{ 
    cout << "-------------------------------------------" << endl;
    cout << "number of priority: " << pri_num << endl;
    cout << "-------------------------------------------" << endl;

    for ( int i=0 ; i< pri_num ; i++ ){
        cout << i << " priority: (" << cache[i].get_remain() << ")" << endl;
        cache[i].display();
        cout << "-------------------------------------------" << endl;
    }
}

void PriorityCache::replace(string x, int pri, vector <string> content) 
{
    int size = content.size() * content.at(0).size();
    if ( capacity < size ){
        cout << "error: content is larger than cache size" << endl;
        return;
    }

    //cout << x << " " << currentCache.get_remain() << endl;
    if( pri==pri_num-1 ){
        //cout << x << " " << cache[pri].get_remain() << endl;
        cache[pri].add(x, content);
        return;
    }
    else {
        for( int i=pri ; i<pri_num ; i++){
            if( cache[i].get_remain() > size){
                cache[i].add( x, content);
                break;
            }
            else{
                while ( cache[i].get_remain() > size){
                    string pop_name;
                    vector <string> pop_content;
                    pop_content = cache[i].pop( pop_name );
                    replace( pop_name, i+1, pop_content );
                }
                cache[i].add( x, content );
            }
        }
    }

    return;
}
