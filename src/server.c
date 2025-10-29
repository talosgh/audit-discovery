#include "server.h"

#include "log.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

int http_server_run(int port, void (*handler)(int client_fd, void *ctx), void *ctx) {
    if (!handler) {
        log_error("No HTTP handler provided");
        return 0;
    }

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        log_error("socket failed: %s", strerror(errno));
        return 0;
    }

    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        log_error("setsockopt failed: %s", strerror(errno));
        close(server_fd);
        return 0;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);

    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        log_error("bind failed: %s", strerror(errno));
        close(server_fd);
        return 0;
    }

    if (listen(server_fd, 64) < 0) {
        log_error("listen failed: %s", strerror(errno));
        close(server_fd);
        return 0;
    }

    log_info("Webhook server listening on port %d", port);

    for (;;) {
        int client_fd = accept(server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            log_error("accept failed: %s", strerror(errno));
            break;
        }

        handler(client_fd, ctx);
        close(client_fd);
    }

    close(server_fd);
    return 1;
}
