/*
 * Client.cpp
 *
 *  Created on: Sep 25, 2014
 *      Author: wangli
 */
#include <stdlib.h>
#include <unistd.h>

#include <fstream>
#include <string>
#include <stdio.h>
#include "Client/ClientResponse.h"
#include "common/Block/ResultSet.h"
#include "Client/Client.h"
#include "common/Logging.h"
#include "startup.h"
#include "utility/command_line.h"
#include "utility/rdtsc.h"

void readStrigFromTerminal(string & input){
	while(true){
		std::cin.clear();
		std::cin.sync();
		std::string str;
		if(getline(std::cin,str)){
			bool finish=false;
			for(unsigned i=0;i<str.length();i++){
				if(str[i]==';'){
					input+=str.substr(0,i+1);
					finish=true;
					break;
				}

			}
			if(finish)
				break;
			input+=str+" ";
		}
	}
}

void submit_command(Client& client, std::string &commands){
	ResultSet rs;
	std::string message;
	/*
	 * split a string that consist of many SQL statements into many string,
	 * and commit one by one
	 */
	string::size_type last_command_end = 0;	// index of last ';' + 1
	string::size_type cur_command_end = 0;	//
	while ((cur_command_end = commands.find(';', last_command_end)) != string::npos) {
		string sql_command = commands.substr(last_command_end, cur_command_end-last_command_end+1);
		std::cout<<sql_command<<endl;
		switch(client.submit(sql_command,message,rs)){
		case Client::result:
			rs.print();
			//				if(i!=0)
			//					total_time+=rs.query_time_;
			break;
		case Client::message:
			printf("%s",message.c_str());
			break;
		case Client::error:
			printf("%s",message.c_str());
			break;
		default:
			assert(false);
			break;
		}

		last_command_end = cur_command_end + 1;
	}

}

void submit_command_repeated(Client& client, std::string &command,int repeated){
	double total_time=0;
	for(int i=0;i<repeated;i++){
		ResultSet rs;
		std::string message;
		switch(client.submit(command,message,rs)){
		case Client::result:
//			rs.print();
			if(i!=0)
				total_time+=rs.query_time_;
			break;
		case Client::message:
			printf("%s",message.c_str());
			break;
		case Client::error:
			printf("%s",message.c_str());
			break;
		default:
			assert(false);
			break;
		}
	}
}


int main(int argc, char** argv){
	/* Client */

	if(argc!=3){
		printf("argc=%d, Illegal input. \nPlease use client master_ip client_listener_port.\n",argc);
		printf("HINT: the master ip and the client_listener_port can be found in the configure file.\n");
		return 0;
	}

	print_welcome();

	Client client;
	client.connection(argv[1], atoi(argv[2]));
	std::cout << std::endl;
	init_command_line();


	while(1){
		std::string command,message;

		get_one_command(command);

		command=trimSpecialCharactor(command);

		if( command == "exit;"||command=="shutdown;" ){
			break;
		}else if( command.empty() ){
			continue;
		}

		submit_command(client,command);

		/*
		 * the following command execute the query for a given time and p
		 * rint the averaged query response time.*/
//		submit_command_repeated(client,command,50);
	}
	client.shutdown();
}


