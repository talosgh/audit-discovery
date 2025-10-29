#ifndef SERVER_H
#define SERVER_H

int http_server_run(int port, void (*handler)(int client_fd, void *ctx), void *ctx);

#endif /* SERVER_H */
