#include<iostream>
#include<string>
#include<fstream>
#include<istream>
#include<sstream>
#include<map>
#include<stdio.h>
#include<omp.h>
#include<cmath>
#include<vector>
#include <algorithm>  
using namespace std;


#define N_THREADS 4
  int main()
  {
	double d = 0.85; 
  int max_node=0; //number of nodes
	int n1, n2,temp;
	map<int, vector<int> > mymap;		
	ifstream f;	
	f.open("facebook_combined.txt");
	if(!f){
		 cout<<"Errror opening file";
		return 0;
	}
	f >> n1 >> n2;
	while(!f.eof())
	{
		if ( n1  > n2 ) {
		if ( temp < n1 ) {
			temp = n1;
			}
			}
		if ( n2 > n1 ) {
			if ( temp < n2 ) {
					temp =n2; 			
}				
			}
		f >> n1 >> n2;
	}
	f.close();
	max_node  = temp+1;
	//Outlink graph for the input.
	vector<vector <double> > Outlinks(max_node);

	//adjacaceny matrix for our caculations..
	vector<vector<double>> M(max_node,vector<double>(max_node,0.0)); 
				 
cout << max_node;
	//find the number of nodes..basically getting the node with highest number
	ifstream myfile  ("facebook_combined.txt");
        for (std::string line; std::getline(myfile, line); )
		{
 		   std::istringstream iss(line);
		   int index = line.find(" ");
		   string node_one = line.substr(0,index);
		   string node_two = line.substr(index);
		   double node = double(std::stoi( node_one ));
		   double node_inttwo = double(std::stoi( node_two ));
		   Outlinks[node].push_back(node_inttwo);
		   Outlinks[node_inttwo].push_back(node);
		 //  cout << "node one : " << node << "node two :" << node_inttwo <<endl;
		}

	//rank vecotr initialized with the value (1-d)/N for every node..	
	//double d_value = 1-d;
	double d_value = 1-d;
	//double max_nodes_value = max_node+1;
	double initial_value= d_value/max_node;
	//cout << "initial_value:" <<  initial_value <<endl;
	vector<double> ranks(max_node,initial_value);
	vector<double> ranksnew(max_node, 0);
	
	//making our Adjacaency matrix from the outlink graph derived above.
	
	for ( int i =0 ; i < max_node ; i++) {
		int j =0 ;
		while ( j < Outlinks[i].size() ) {
			int index = Outlinks[i][j];
			double outlinksize = Outlinks[index].size();
			//cout << "index: " << index << "outlinksize : "<< outlinksize << endl;
			double base =  1/outlinksize;
			if ( M[i][index] == 0 ) {
			//cout << "the base is:" << base;
			M[i][index] = base;}
			j++;		
		}
	}
	//cout<<endl;
	//cout<<endl;
	/*for ( int i =0 ; i < max_node ; i++) {
		for ( int j =0 ; j < max_node ; j++) {
			//cout<< M[i][j]<<"	";
		}
		cout<<endl;
	}/*/
	double sum= 0.0;
	
	//  r'=Mr and this needs to go on for atleast 8 iterations.
	int iteration = 0 ;
	
	//cout<<endl;
	//Main loop to control the numner of iterations  of iterations ..
	//later we can replace with threshold conditon.

	while ( iteration < 96 ){
#pragma omp parallel shared(M,ranks,ranksnew) num_threads(N_THREADS)
{
#pragma omp for //lastprivate(ranks)
		for (int i = 0 ; i < max_node; i++){
			double sum=0.15/max_node;
			//double sum=0.0;
			#pragma omp parallel for reduction(+:sum)
			for ( int j =0 ; j < max_node ; j++ ) {
				#pragma omp atomic	
				sum   += (M[i][j]*ranks[j]);
								
			}
			ranksnew[i] = (0.85)*sum;	
			//ranksnew[i] = sum;		
		}
}//end of parallel stuff.
		//cout<<"Iteration :"<<(iteration+1)<<endl;
		ranks = ranksnew;
		iteration++;
		//cout<<endl;
	}

		int num = 0;
		double sumranks =0.0;
	/*	while ( num < max_node){
			sumranks += ranks[num]; 
		        cout << "sum is : " << ranks[num] << endl;
			num++;  
			} */
     multimap<double,int> finalRanks;
	while ( num < max_node) {
				if(ranks[num] && num){ 
			finalRanks.emplace(std::pair<double,int>(ranks[num],num));}
		num++;
}
 for (auto& x: finalRanks){
    std::cout << " [" << x.first << ':' << x.second << ']';
  std::cout << '\n';
}

 ofstream outfile("output_task1.txt");

        multimap<double,int >::iterator myit=finalRanks.begin();
        for (myit = finalRanks.begin(); myit != finalRanks.end(); myit++)
        {
            outfile<< myit->first << "\t" << myit->second << "\n";
        }
       outfile.close();

		return 0;
  }
