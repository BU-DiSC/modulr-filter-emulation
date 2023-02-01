#pragma once
#include <bits/stdc++.h> 
#include "LRUcache.h" 
using namespace std; 

class PriorityCache { 
    // store keys of cache 
    list<string> dq; 
  
    // store references of key in cache 
    unordered_map<string, list<string>::iterator> ma; 
    unordered_map<string, vector <string>> buffer; 
    unordered_map<string, int> priority; 

    int capacity; // maximum capacity of cache 

    int pri_num;  // number of priority
    int csize;    // bytes in the cache
    int * size_ratio;
    int * num_elm;

    vector <LRUCache> cache;

public:
    PriorityCache(int size); 
    PriorityCache(int size, int pri); 
    vector<string> refer(string input_str, int pri); 
    void add(string block_name, int pri, vector<string> block); 
    void replace(string block_name, int pri, vector<string> block); 
    void display(); 
};
