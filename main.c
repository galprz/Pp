#include <stdio.h>
#include <unistd.h> 
#include <stdio.h> 
#include <sys/socket.h> 
#include <stdlib.h> 
#include <netinet/in.h> 
#include <string.h> 
#include <pthread.h>
#include <stdarg.h>

#define PORT 8080
#define CLIENT_PORT 7777

#define SHOLD_LOG 1
#define SERVERS_SIDE_IP
enum _JobType{
    Video=0, Music=1, Picture=2
};
typedef enum _JobType JobType;
typedef struct server Server;

struct server{
    int server_fd;
    JobType job_type;
    int load;
    pthread_mutex_t lock;
};

void log(const char *fmt, ...) {
    if (SHOLD_LOG){
        va_list args;
        va_start(args, fmt);
        vprintf(fmt, args);
        va_end(args);
    }
}

int create_server_socket(char* ip_addr){
    int server_fd, new_socket, valread; 
    struct sockaddr_in address; 
    int opt = 1; 
    int addrlen = sizeof(address); 
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) 
    { 
        perror("socket failed"); 
        exit(EXIT_FAILURE); 
    }
      // Forcefully attaching socket to the port 8080 
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, 
                                                  &opt, sizeof(opt))) 
    { 
        perror("setsockopt"); 
        exit(EXIT_FAILURE); 
    } 
    address.sin_family = AF_INET; 
    address.sin_addr.s_addr = inet_addr("192.168.14.148");
    address.sin_port = htons( CLIENT_PORT ); 
    // Forcefully attaching socket to the port 8080 
    if (bind(server_fd, (struct sockaddr *)&address,  
                                 sizeof(address))<0) 
    { 
        perror("bind failed"); 
        exit(EXIT_FAILURE); 
    }
    struct sockaddr_in remoteaddr;
    remoteaddr.sin_family = AF_INET;
    remoteaddr.sin_addr.s_addr = inet_addr(ip_addr);
    remoteaddr.sin_port = htons(PORT);
    connect(server_fd, (struct sockaddr *)&remoteaddr, sizeof(remoteaddr));
    return server_fd;
}

int create_client_socket(){
    int server_fd, new_socket, valread; 
    struct sockaddr_in address; 
    int opt = 1; 
    int addrlen = sizeof(address); 
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) 
    { 
        perror("socket failed"); 
        exit(EXIT_FAILURE); 
    }
      // Forcefully attaching socket to the port 8080 
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, 
                                                  &opt, sizeof(opt))) 
    { 
        perror("setsockopt"); 
        exit(EXIT_FAILURE); 
    } 
    address.sin_family = AF_INET; 
    address.sin_addr.s_addr = inet_addr("192.168.14.148");; 
    address.sin_port = htons( PORT ); 
       
    // Forcefully attaching socket to the port 8080 
    if (bind(server_fd, (struct sockaddr *)&address,  
                                 sizeof(address))<0) 
    { 
        perror("bind failed"); 
        exit(EXIT_FAILURE); 
    }
    listen(server_fd, 5);
    return server_fd;
}

int calculate_job_time(JobType server_type, JobType job_type, int current_load){
    if (server_type == Video && (job_type == Video || job_type == Picture)){
        return current_load;
    }
    if (server_type == Video && job_type == Music){
        return 2 * current_load;
    }
    if (server_type == Music && job_type == Music){
        return current_load;
    }
    if (server_type == Music && job_type == Picture){
        return 2*current_load;
    }
    if (server_type == Music && job_type == Video){
        return 3*current_load;
    }

}

JobType extract_job_type(char* message){
    if(message[0]=='M'){
        return Music;
    }
    if(message[0]=='V'){
        return Video;
    }
    return Picture;
}

int extract_load(char* message){
    return atoi(message + 1);
}

int select_server(Server * servers, JobType job_type, int current_load){
    int min_load = -1;
    int selected_server_index = -1;
    int first_server = rand() % 10;
    int second_server = rand() % 9;
    second_server = (second_server + first_server) % 10;
    int first_server_job_time = calculate_job_time(servers[first_server].job_type, job_type, current_load);
    int second_server_job_time = calculate_job_time(servers[second_server].job_type, job_type, current_load);

    log("first: %d type %d, second %d type %d\n", first_server, servers[first_server].job_type, 
                                            second_server, servers[second_server].job_type);
    pthread_mutex_lock(&(servers[first_server].lock));
    pthread_mutex_lock(&(servers[second_server].lock));
        int future_load_server_one = servers[first_server].load + calculate_job_time(servers[first_server].job_type, job_type, current_load);
        int future_load_server_two = servers[second_server].load + calculate_job_time(servers[second_server].job_type, job_type, current_load);

        log("first server future load: %d\n", future_load_server_one);
        log("second server future load: %d\n", future_load_server_two);

        int selected_index = (future_load_server_one < future_load_server_two) ?
                            first_server: second_server;
        servers[selected_index].load = min(future_load_server_one, future_load_server_two);
    pthread_mutex_unlock(&(servers[second_server].lock));
    pthread_mutex_unlock(&(servers[first_server].lock));
    return selected_index;
}

struct payload
{
    char * client_message;
    int client_fd;
    Server* server;
};

typedef struct payload Payload;


void _handle_connection(void* payload){
    Payload * payload_ptr = (Payload *) payload;
    int server_fd = payload_ptr->server->server_fd;
    int current_index = 0;
    int read_size, send_size;
    read_size = send_size = 0;

    // send to server
    while( (send_size = send(server_fd , payload_ptr->client_message + current_index , strlen(payload_ptr->client_message + current_index) , 0 )) > 0 ){
        log(payload_ptr->client_message + current_index);
        current_index += send_size;
    }
    current_index = 0;
    char server_message[100] = {0};

    while( (read_size = recv(server_fd , server_message + current_index , 100 , 0)) > 0 ){
        log(server_message + current_index);
        current_index += read_size;
    }
    current_index = 0;
    send_size = 0;
    while( (send_size = send(payload_ptr->client_fd , server_message + current_index , strlen(server_message + current_index) , 0 )) > 0 ){
        log(server_message + current_index);
        current_index += send_size;
    }
    int current_load = extract_load(payload_ptr->client_message);
    JobType job_type = extract_job_type(payload_ptr->client_message);
    int load = calculate_job_time(payload_ptr->server->job_type, job_type, current_load);
    pthread_mutex_lock(&(payload_ptr->server->lock));
        payload_ptr->server->load -= load;
    pthread_mutex_unlock(&(payload_ptr->server->lock));
}

void handle_connection(char * client_message, int client_fd, Server* server){
    pthread_t thread;
    Payload* payload = malloc(sizeof(Payload));
    payload->client_fd = client_fd;
    payload->server = server;
    payload->client_message = client_message;
    pthread_create( &thread, NULL, _handle_connection, (void*) payload);
}

int main(){
    Server* servers = malloc(sizeof(Server) * 10);
    for (int i=0; i<10; i++){
        char ip[80];
        sprintf(ip, "192.168.0.10%d",i);
        JobType job_type = (i<6) ? Video : Music;
        servers[i].server_fd = create_server_socket(ip);
        servers[i].job_type = job_type;
        servers[i].load = 0;
        pthread_mutex_init(&servers[i].lock, NULL);
    }

    struct sockaddr_in my_addr, peer_addr, peer_addr_size;
    int clients_fd = create_client_socket();
    while(1){
        log("Waiting for connections\n");
        int acc = accept(clients_fd, (struct sockaddr *)&peer_addr, &peer_addr_size);
        if(acc > 0) {
            log("Accepted\n");
            log("Waiting for job\n");
            char * client_message = (char*) malloc(sizeof(char)* 10);
            for (int i = 0 ;i <10; i++){ client_message[i] = 0;}
            int read_size;
            int current_index = 0;
            while( (read_size = recv(acc , client_message + current_index , 10 , 0)) > 0 ){
                //Send the message back to client
                log(client_message + current_index);
                current_index += read_size;
            }
            
            JobType job_type = extract_job_type(client_message);
            int load =  extract_load(client_message);
            log("Job type %d, load %d \n", job_type, load);
            int server_id = select_server(servers, job_type, load);
            handle_connection(client_message, acc, servers + server_id);
            log("Selected server %d \n", server_id);
        }else {
            log("Not accepted\n");
        }
    }
}