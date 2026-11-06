#define _POSIX_C_SORCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <netinet/in.h>
#include <arpa/inet.h>

static volatile sig_atomic_t got_sighup = 0;
static volatile sig_atomic_t stop_requested = 0;

static void handle_signal(int signo){
    if(signo == SIGHUP){
        got_sighup = 1;
    } else if(signo == SIGHUP || signo == SIGTERM){
        stop_requested = 1;
    }
}

static int set_nonblocking(int fd){
    int flags = fcntl(fd, F_GETFL);
    if(flags == -1){
        return -1;
    }
    return fcntl(fd, F_GETFL, flags | O_NONBLOCK);
}

int main(void) {
    int server_fd = -1;
    int client_fd = -1;
    int exit_status = EXIT_FAILURE;
    int reuse = 1;

    struct sockaddr_in server_addr;
    struct sigaction sa;
    sigset_t blocked_signals;
    sigset_t original_mask;
    sigset_t wait_mask;

    // 1 Блокируем сигналы до установки обработчиков.
    sigemptyset(&blocked_signals);
    sigaddset(&blocked_signals, SIGHUP);
    sigaddset(&blocked_signals, SIGINT);
    sigaddset(&blocked_signals, SIGTERM);

    if (sigprocmask(SIG_BLOCK, &blocked_signals,
                    &original_mask) == -1) {
        perror("sigprocmask");
        return EXIT_FAILURE;
    }

    // 2 Устанавливаем обработчик сигналов.
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_signal;
    sigemptyset(&sa.sa_mask);

    if (sigaction(SIGHUP, &sa, NULL) == -1 ||
        sigaction(SIGINT, &sa, NULL) == -1 ||
        sigaction(SIGTERM, &sa, NULL) == -1) {
        perror("sigaction");
        goto cleanup;
    }

    // Маска используемая только на время pselect().
    wait_mask = original_mask;
    sigdelset(&wait_mask, SIGHUP);
    sigdelset(&wait_mask, SIGINT);
    sigdelset(&wait_mask, SIGTERM);

    // 3 Создаём TCP-сокет.
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == -1) {
        perror("socket");
        goto cleanup;
    }

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR,
                   &reuse, sizeof(reuse)) == -1) {
        perror("setsockopt");
        goto cleanup;
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons(5555);

    if (bind(server_fd, (struct sockaddr *)&server_addr,
             sizeof(server_addr)) == -1) {
        perror("bind");
        goto cleanup;
    }

    if (listen(server_fd, 5) == -1) {
        perror("listen");
        goto cleanup;
    }

    if (set_nonblocking(server_fd) == -1) {
        perror("fcntl server");
        goto cleanup;
    }

    printf("Server is listening on port 5555\n");
    printf("Send SIGHUP to test signal handling.\n");

    // 4 Единый цикл ожидания
    while (!stop_requested) {
        fd_set readfds;
        int max_fd = server_fd;

        FD_ZERO(&readfds);
        FD_SET(server_fd, &readfds);

        if (client_fd != -1) {
            FD_SET(client_fd, &readfds);

            if (client_fd > max_fd) {
                max_fd = client_fd;
            }
        }

        int ready = pselect(max_fd + 1, &readfds,
                            NULL, NULL, NULL, &wait_mask);

        // Сигналы обрабатываем в основном потоке
        if (got_sighup) {
            got_sighup = 0;
            printf("Received SIGHUP\n");
        }

        if (stop_requested) {
            break;
        }

        if (ready == -1) {
            if (errno == EINTR) {
                continue;
            }

            perror("pselect");
            goto cleanup;
        }

        // 5. Появилось новое подключение
        if (FD_ISSET(server_fd, &readfds)) {
            struct sockaddr_in peer;
            socklen_t peer_len = sizeof(peer);

            int new_fd = accept(
                server_fd,
                (struct sockaddr *)&peer,
                &peer_len
            );

            if (new_fd == -1) {
                if (errno != EAGAIN && errno != EWOULDBLOCK &&
                    errno != EINTR) {
                    perror("accept");
                }
            } else {
                char ip[INET_ADDRSTRLEN];

                if (inet_ntop(AF_INET, &peer.sin_addr,
                              ip, sizeof(ip)) == NULL) {
                    snprintf(ip, sizeof(ip), "unknown");
                }

                printf("New connection: %s:%u\n",
                       ip, (unsigned)ntohs(peer.sin_port));

                if (client_fd == -1) {
                    if (set_nonblocking(new_fd) == -1) {
                        perror("fcntl client");
                        close(new_fd);
                    } else {
                        client_fd = new_fd;
                        printf("Client accepted\n");
                    }
                } else {
                    printf("Extra connection - closing it\n");
                    close(new_fd);
                }
            }
        }

        // 6 Появились данные или клиент отключился
        if (client_fd != -1 && FD_ISSET(client_fd, &readfds)) {
            char buffer[4096];

            ssize_t n = recv(client_fd, buffer,
                             sizeof(buffer), 0);

            if (n > 0) {
                printf("Received %zd bytes\n", n);
            } else if (n == 0) {
                printf("Client disconnected\n");
                close(client_fd);
                client_fd = -1;
            } else if (errno != EAGAIN &&
                       errno != EWOULDBLOCK &&
                       errno != EINTR) {
                perror("recv");
                close(client_fd);
                client_fd = -1;
            }
        }
    }

    printf("Server is shutting down\n");
    exit_status = EXIT_SUCCESS;

cleanup:
    if (client_fd != -1) {
        close(client_fd);
    }

    if (server_fd != -1) {
        close(server_fd);
    }

    if (sigprocmask(SIG_SETMASK, &original_mask, NULL) == -1) {
        perror("sigprocmask restore");
        exit_status = EXIT_FAILURE;
    }

    return exit_status;
}
