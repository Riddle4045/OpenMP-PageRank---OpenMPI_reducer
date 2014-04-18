#include <iostream>
#include <sstream>
#include <mpi.h>
#include <vector>
#include <map>
#include <string>
using namespace std;

int main(int argc, char *argv[])
{
    char filename[128];
    const int overlap=100;
    MPI::File thefile;
    MPI::Offset localsize;
    MPI::Offset start, end;
    char *chunk;

    MPI::Init(argc,argv);
    int rank = MPI::COMM_WORLD.Get_rank();
    int size = MPI::COMM_WORLD.Get_size();
	//cout << "size is:" << size << "\t";
    if(argc != 2) {

        MPI::Finalize();
        return 1;
    }
    thefile = MPI::File::Open(MPI::COMM_WORLD,
            argv[1], MPI::MODE_RDONLY, MPI::INFO_NULL);
    MPI::Offset filesize = thefile.Get_size();  // in bytes
    localsize = filesize / size;   // local number to read

    start = rank * localsize;
    end   = start + localsize - 1;

    /* add overlap to the end of everyone's chunk... */
    end += overlap;

    /* except the last processor, of course */
    if (rank == size-1) end = filesize;

    localsize =  end - start + 1;

    /* allocate memory */
    chunk = (char *)malloc( (localsize + 1)*sizeof(char));

    thefile.Read_at(start, chunk, localsize, MPI_CHAR);
    chunk[localsize] = '\0';

    int locstart=0, locend=localsize;
    if (rank != 0) {
        while(chunk[locstart] != '\n') locstart++;
        locstart++;
    }
    if (rank != size-1) {
        locend-=overlap;
        while(chunk[locend] != '\n') locend++;
    }
    localsize = locend-locstart+1;


    char *data = (char *)malloc((localsize+1)*sizeof(char));
    memcpy(data, &(chunk[locstart]), localsize);
    data[localsize] = '\0';
    free(chunk);

    std::vector<string> linesVector;


    int nlines = 0;
    for (int i=0; i<localsize; i++)
        if (data[i] == '\n') nlines++;

    //cout<<"rank : "<<rank<<" lines : "<<nlines<<endl;


    char * s = strtok(data,"\n");
    std::string str(s);
    linesVector.push_back(s);

    for (int i=1; i<nlines; i++) {
        s = strtok(NULL, "\n");
        std::string str1(s);
        linesVector.push_back(str1);
    }

    map<int , int > keyValuePair;
    int count =0;
    for (int i=0; i < linesVector.size(); i++) {
        string line;
        line = linesVector[i];
        //printf("%d: <%s>: %f + %f = %f\n", rank, lines[i], a, b, a+b);
        int index = line.find(",");
        int sum = 0;
        string node_one = line.substr(0,index);
        string node_two = line.substr(index+1);
        if ( node_one != "key" ) {
               //count incremented here ..so that only the lines with keyValue pairs are counted
            count++;
            int node = std::stoi( node_one );
            int node_inttwo = std::stoi( node_two );
            if ( keyValuePair.find(node) != keyValuePair.end()) {
                keyValuePair.at(node) += node_inttwo;
            } else {
                keyValuePair.insert(std::pair<int,int>(node,node_inttwo));
            }

        }
    }
    //cout<<"rank : "<<rank<<" Keyvalue pair map size :"<<keyValuePair.size()<<endl;
	//a buffer with that holds string's to be sent to each prcessor.
	vector<string> buffer(size);
	// key % size = x -- > x owner of the key "key"
	for(std::map<int,int>::iterator it = keyValuePair.begin(); it!=keyValuePair.end(); it++){
		int index = (it->first) % size;
	//	cout << " the index value  is :" << index << "\n";
		string line = to_string(it->first) + "-" + to_string(it->second)+"," ;
		//cout << "the line being sent is :" << line << "\n";

        //printf("%s\n",line.c_str());
		buffer[index] += line;
	}  


    // Send msgs to all other processes with data.
	string bufferstring ;
	for ( int i=0 ; i < size; i++ ) {
		const char* bufferstring = buffer[i].c_str();
        int sizeOfBuffer = strlen(bufferstring);    
		MPI::COMM_WORLD.Isend(bufferstring, sizeOfBuffer, MPI::CHAR, i, 1);
    }


    map<int,int> finalValues;
    int dataFrom = 0;

    // Receive data from all other processes 
	while (dataFrom < size ) {
        MPI::Status status;
	    int sizeOfBuffer; 

        MPI::COMM_WORLD.Probe(dataFrom,1,status);
	    sizeOfBuffer = status.Get_count(MPI_CHAR); 
	    char* buf = new char[sizeOfBuffer]; 
        //cout<<"from "<< status.Get_source()<<"size of buffer : "<<sizeOfBuffer<<endl;
	    MPI::COMM_WORLD.Recv(buf, sizeOfBuffer, MPI_CHAR, dataFrom, 1);
	    string str(buf, sizeOfBuffer); 
        
	    //cout << "received " << str << "\n";
        
        for(int i = 0; i < str.size(); i++) {
            int pos = str.find("-", i);
            if(pos != std::string::npos) {
                int pos1 = str.find(",",i);

                string node_one = str.substr(i,pos-i);
                string node_two = str.substr(pos+1,pos1-pos-1);

                //printf("%s  %s\n",node_one.c_str(), node_two.c_str());
                int node1 = atoi( node_one.c_str());
                int node2 = atoi( node_two.c_str());
                
                double aggregate = 0;
                if ( finalValues.find(node1) != finalValues.end()) {
                
			    finalValues.at(node1) += node2;
                   // cout << "i am here";
                } else {
                    finalValues.insert(std::pair<int,int>(node1,node2));
                    
                }  
                i = pos1; 
            }
        }
	    dataFrom ++;
//empty the buffer values...
        free(buf);
    } 

    for (std::map<int,int>::iterator it=finalValues.begin(); it!=finalValues.end(); ++it){       
        printf("%d, %d\n",it->first, it->second);
    }
    
    thefile.Close();
    MPI::Finalize();
    return 0;
} 
