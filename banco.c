#include "banco_comun.h"
#include <sys/stat.h>

// Nuevos includes para la parte 2
#include<netinet/in.h>
#include<sys/socket.h>
#include<arpa/inet.h>

/* Variables globales */
static mqd_t g_mq_log     = -1; //cola para recibir log desde hijos
static mqd_t g_mq_alerta  = -1;//cola recibir alertas desde monitor
static mqd_t g_mq_monitor = -1; //cola por la que se comunican datos hacia el monitor

static pid_t g_monitor_pid = -1;

#define MAX_HIJOS 64 // Maximo numero de hijos
//tabla procesos hijos
typedef struct {
    pid_t pid;
    int   cuenta_id;
    int   pipe_wr;
} InfoHijo;
typedef struct {
    pid_t pid_usuario;
    int   cuenta_id;
    int   pipe_wr;
} RegistroHijo;
static InfoHijo g_hijos[MAX_HIJOS];
static int      g_num_hijos = 0;

static Config g_cfg; // almacena la configuración del sistema
static volatile sig_atomic_t g_salir = 0;  //volatile porque se modifica desde un manejador de señal

/* Señales */
static void manejador_sigterm(int s) { (void)s; g_salir = 1; }

/* Leer config.txt  */
static int leer_config(const char *ruta, Config *cfg) {
    FILE *f = fopen(ruta, "r");
    if (!f) { perror("fopen config"); return -1; }
    //
    //
    //
    char linea[256];

memset(cfg, 0, sizeof(Config)); //estructura a cero

while (fgets(linea, sizeof(linea), f)) { //comentarios y lineas vacias
        if (linea[0] == '#' || linea[0] == '\n') {
        continue;
    }

    char clave[64], valor[256]; //divide la linea en clave y valor
    if (sscanf(linea, "%63[^=]=%255[^\n]", clave, valor) != 2) { //ssacnf - leer/extraer datos de cadena texto
        continue;
    }

    if (strcmp(clave, "PROXIMO_ID") == 0) {// comparar cadenas
        cfg->proximo_id = atoi(valor); //cfg - puntero a config
    } else if (strcmp(clave, "LIM_RET_EUR") == 0) {
        cfg->lim_ret_eur = (float)atof(valor);
    } else if (strcmp(clave, "LIM_RET_USD") == 0) {
        cfg->lim_ret_usd = (float)atof(valor);
    } else if (strcmp(clave, "LIM_RET_GBP") == 0) {
        cfg->lim_ret_gbp = (float)atof(valor);
    } else if (strcmp(clave, "LIM_TRF_EUR") == 0) {
        cfg->lim_trf_eur = (float)atof(valor);
    } else if (strcmp(clave, "LIM_TRF_USD") == 0) {
        cfg->lim_trf_usd = (float)atof(valor);
    } else if (strcmp(clave, "LIM_TRF_GBP") == 0) {
        cfg->lim_trf_gbp = (float)atof(valor);
    } else if (strcmp(clave, "UMBRAL_RETIROS") == 0) {
        cfg->umbral_retiros = atoi(valor);
    } else if (strcmp(clave, "UMBRAL_TRANSFERENCIAS") == 0) {
        cfg->umbral_transferencias = atoi(valor);
    } else if (strcmp(clave, "NUM_HILOS") == 0) {
        cfg->num_hilos = atoi(valor);
    } else if (strcmp(clave, "ARCHIVO_CUENTAS") == 0) {
        strncpy(cfg->archivo_cuentas, valor, MAX_PATH - 1);
    } else if (strcmp(clave, "ARCHIVO_LOG") == 0) {
        strncpy(cfg->archivo_log, valor, MAX_PATH - 1);
    } else if (strcmp(clave, "CAMBIO_USD") == 0) {
        cfg->cambio_usd = (float)atof(valor);
    } else if (strcmp(clave, "CAMBIO_GBP") == 0) {
        cfg->cambio_gbp = (float)atof(valor);
    }else if (strcmp(clave, "PUERTO") == 0){
        cfg->puerto = atoi(valor);
    }
}
    //
    //
    //

    fclose(f);
    return 0;
}

/* Actualizar PROXIMO_ID en config.txt */
static void guardar_proximo_id(int nuevo_id) {
    FILE *f = fopen("config.txt", "r");
    if (!f) return;
    char contenido[4096];
    size_t n = fread(contenido, 1, sizeof(contenido)-1, f);
    contenido[n] = '\0';
    fclose(f);

    char *p = strstr(contenido, "PROXIMO_ID=");
    if (!p) return;
    size_t antes     = (size_t)(p - contenido);
    char  *fin_linea = strchr(p, '\n');

    char nuevo[4096];
    snprintf(nuevo, sizeof(nuevo), "%.*sPROXIMO_ID=%d%s",
             (int)antes, contenido, nuevo_id,
             fin_linea ? fin_linea : "");

    f = fopen("config.txt", "w");
    if (!f) return;
    fputs(nuevo, f);
    fclose(f);
}

/* Buscar cuenta */
static int buscar_cuenta(int numero, Cuenta *c) {
    FILE *f = fopen(g_cfg.archivo_cuentas, "rb");
    if (!f) return 0;
    int ok = 0;
    while (fread(c, sizeof(Cuenta), 1, f) == 1)
        if (c->numero_cuenta == numero) { ok = 1; break; }
    fclose(f);
    return ok;
}

/* Crear nueva cuenta */
static int crear_cuenta(Cuenta *nueva) {
    sem_t *sc = sem_open(SEM_CONFIG, 0);
    //
    //
    //
    if (sc == SEM_FAILED) { perror("sem_open config"); return -1; }

sem_wait(sc); //asegura la exclusión mutua

nueva->numero_cuenta = g_cfg.proximo_id;
g_cfg.proximo_id++;

guardar_proximo_id(g_cfg.proximo_id); //guarda en config el nuevo valor de id

sem_post(sc); //termina, lobera el semáforo
sem_close(sc); // el proceso deja de usarlo
    //
    //
    // 
    sem_t *sa = sem_open(SEM_CUENTAS, 0); //abre semáforo cuentas
    //
    //
    //
    if (sa == SEM_FAILED) { perror("sem_open cuentas"); return -1; }


sem_wait(sa); //exlusión mutua

FILE *f = fopen(g_cfg.archivo_cuentas, "ab"); //escribir al final y binario
if (!f) { //comprueba si el archivo no se ha podido abrir
    perror("fopen cuentas");
    sem_post(sa);
    sem_close(sa);
    return -1;
}

if (fwrite(nueva, sizeof(Cuenta), 1, f) != 1) { //se escribe nueva cuenta en el fichero
    perror("fwrite cuentas");
    fclose(f);
    sem_post(sa);
    sem_close(sa);
    return -1;
}
fclose(f);
sem_post(sa);
sem_close(sa);
    //
    //
    //
    return nueva->numero_cuenta;
}

/* Escribir en el log */
static void escribir_log(const char *linea) {
    FILE *f = fopen(g_cfg.archivo_log, "a"); //abrir transacciones y añadir una linea
    //
    // Completar
    if (!f)
    {
        perror("Error al abrir archivo log");
        return;
    }
    fprintf(f, "%s\n", linea);
    // Escribimos indemdiantamente si esperar a que se llene el buffer
    fflush(f);
    fclose(f);
    //
}

/* Buscar hijo por cuenta_id */
static InfoHijo *buscar_hijo(int cuenta_id) {
    for (int i = 0; i < g_num_hijos; i++)
        if (g_hijos[i].cuenta_id == cuenta_id) return &g_hijos[i];
    return NULL;
}

/* Procesar mensajes de log pendientes en MQ_LOG */
static void procesar_log(void) {
    DatosLog dl;
    while (mq_receive(g_mq_log, (char *)&dl, sizeof(dl), NULL) > 0) {
    //
        // Completar
        char linea[512];
        // Obtenemos el nombre de la operación según su tipo (depósito, retiro, etc.)
        const char *op;
        if (dl.tipo_op == OP_DEPOSITO)
        {
            op = "Deposito";
        }
        else if (dl.tipo_op == OP_RETIRO)
        {
            op = "Retiro";
        }
        else if (dl.tipo_op == OP_TRANSFERENCIA)
        {
            op = "Transferencia";
        }
        else if (dl.tipo_op == OP_MOVER_DIVISA)
        {
            op = "Mover divisa";
        }
        else
        {
            op = "Desconocida";
        }
        // Determinamos el símbolo: + para depósitos, - para el resto de operaciones
        char signo = (dl.tipo_op == OP_DEPOSITO) ? '+' : '-';

        // Construimos la línea del log con el formato requerido por el enunciado
        snprintf(linea, sizeof(linea),
                 "[%s] %s en cuenta %d: %c%.2f %s (PID: %d, Estado: %s)",
                 dl.timestamp,
                 op,
                 dl.cuenta_id,
                 signo,
                 dl.cantidad,
                 nombre_divisa(dl.divisa),
                 dl.pid_hijo,
                 dl.estado == 1 ? "OK" : "FALLIDO");

        // Escribimos la línea formateada en el fichero de log
        escribir_log(linea);
        //
    }
}

/* Procesar alertas pendientes en MQ_ALERTA */
static void procesar_alertas(void) {
    DatosAlerta da;
    while (mq_receive(g_mq_alerta, (char *)&da, sizeof(da), NULL) > 0) {
        char ts[MAX_TS]; timestamp_ahora(ts, sizeof(ts));
        char linea[512];
        int bloquear = (strstr(da.mensaje, ALERTA_RETIROS)        != NULL ||
                        strstr(da.mensaje, ALERTA_TRANSFERENCIAS) != NULL);
        snprintf(linea, sizeof(linea), "[%s] ALERTA: %s - cuenta %s",
                 ts, da.mensaje, bloquear ? "BLOQUEADA" : "monitoreada");
        escribir_log(linea);

        if (bloquear) {
            InfoHijo *h = buscar_hijo(da.cuenta_id);
            if (h && h->pipe_wr != -1) {
                char aviso[512];
                snprintf(aviso, sizeof(aviso),
                         "BLOQUEO: %s en cuenta %d", da.mensaje, da.cuenta_id);
                write(h->pipe_wr, aviso, strlen(aviso)+1);
                close(h->pipe_wr);
                h->pipe_wr = -1;
            }
        }
    }
}



/* Lanzar usuario conectado por socket: redirige stdin/stdout del hijo al socket */
static pid_t lanzar_usuario_socket(int cuenta_id, int socket_fd, int *pipe_wr_out) {
    int pfd[2];
    if (pipe(pfd) < 0) { perror("pipe"); return -1; } // Crear pipe para enviar alertas/bloqueos al hijo

    pid_t pid = fork(); // Crear proceso hijo que gestionará esta conexión
    if (pid < 0) {
        perror("fork usuario_socket");
        close(pfd[0]); close(pfd[1]);
        return -1;
    } else if (pid == 0) { // HIJO
        close(pfd[1]); // El hijo solo lee del pipe, cierra el extremo de escritura

        dup2(socket_fd, STDIN_FILENO);  // Redirigir stdin al socket (el hijo lee del cliente)
        dup2(socket_fd, STDOUT_FILENO); // Redirigir stdout al socket (el hijo escribe al cliente)
        close(socket_fd); // Ya no necesitamos el socket original, lo cerramos

        char arg_cuenta[16];
        char arg_pipe[16];
        snprintf(arg_cuenta, sizeof(arg_cuenta), "%d", cuenta_id);
        snprintf(arg_pipe,   sizeof(arg_pipe),   "%d", pfd[0]);

        // Lanzar el proceso usuario pasándole el id de cuenta y el descriptor del pipe
        execv("./usuario", (char *[]){"./usuario", arg_cuenta, arg_pipe, NULL});
        perror("execv usuario_socket"); // Si llegamos aquí, execv ha fallado
        exit(1);
    } else { // PADRE
        close(pfd[0]); // El padre solo escribe en el pipe, cierra el extremo de lectura
        *pipe_wr_out = pfd[1]; // Devolver el descriptor de escritura para enviar bloqueos al hijo
    }
    return pid;
}

/* Lanzar proceso monitor */
static void lanzar_monitor(void) {
    //
    // Completar
    // Creamos el proceso monitor mediante fork()
    pid_t pid = fork();

    if (pid < 0) {
        // fork() ha fallado, mostramos el error y terminamos el sistema
        perror("fork monitor");
        exit(1);
    } else if (pid == 0) {
        // Estamos en el proceso HIJO (monitor)

        // Reemplazamos la imagen del proceso por el ejecutable monitor
        execv("./monitor", (char *[]){ "./monitor", NULL });

        // Si execv retorna es que ha fallado, mostramos el error y terminamos
        perror("execv monitor");
        exit(1);
    }

    /* Guardamos el PID del monitor para poder terminarlo luego */
    g_monitor_pid = pid;
}

int crear_cuenta_con_id(int id, Cuenta *c) {
    c->numero_cuenta = id;

    FILE *f = fopen(g_cfg.archivo_cuentas, "r+b"); // abrir sin truncar
    if (!f) f = fopen(g_cfg.archivo_cuentas, "wb"); // si no existe, crear
    if (!f) return -1;

    // saltar a la posición del id pedido
    fseek(f, (id - 1) * sizeof(Cuenta), SEEK_SET);
    
    int ok = fwrite(c, sizeof(Cuenta), 1, f) == 1;
    fclose(f);
    return ok ? id : -1;
}

static void atender_cliente_socket(int new_socket, int reg_fd) {
    char *menu =
        "+==============================+\r\n"
        "|       S E C U R E B A N K   |\r\n"
        "+==============================+\r\n"
        "|  Bienvenido al sistema       |\r\n"
        "|  bancario SecureBank         |\r\n"
        "+==============================+\r\n"
        "  Numero de cuenta : ";

    write(new_socket, menu, strlen(menu));

    char buf[64] = {0};
    ssize_t nread = read(new_socket, buf, sizeof(buf) - 1);
    if (nread <= 0) {
        close(new_socket);
        return;
    }
    buf[strcspn(buf, "\r\n")] = '\0';
    int numero = atoi(buf);
    if (numero <= 0) {
        write(new_socket, "Numero de cuenta invalido\r\n", 27);
        close(new_socket);
        return;
    }

    Cuenta cuenta;
    memset(&cuenta, 0, sizeof(cuenta));

    if (!buscar_cuenta(numero, &cuenta)) {
        char msg[] = "Cuenta no encontrada. Nombre del titular: ";
        write(new_socket, msg, strlen(msg));

        char nombre[64] = {0};
        ssize_t n = read(new_socket, nombre, sizeof(nombre) - 1);
        if (n <= 0) {
            close(new_socket);
            return;
        }
        nombre[strcspn(nombre, "\r\n")] = '\0';

        strncpy(cuenta.titular, nombre, sizeof(cuenta.titular) - 1);
        cuenta.saldo_eur = 0.0f;
        cuenta.saldo_usd = 0.0f;
        cuenta.saldo_gbp = 0.0f;

        if (crear_cuenta_con_id(numero, &cuenta) < 0) {
            write(new_socket, "Error al crear cuenta\r\n", 23);
            close(new_socket);
            return;
        }

        char bienvenida[128];
        snprintf(bienvenida, sizeof(bienvenida), "Cuenta %d creada. Bienvenido, %s\r\n", numero, cuenta.titular);
        write(new_socket, bienvenida, strlen(bienvenida));
    }

    int pipe_wr = -1;
    pid_t pid = lanzar_usuario_socket(numero, new_socket, &pipe_wr);
    close(new_socket);

    if (pid > 0) {
        RegistroHijo reg;
        reg.pid_usuario = pid;
        reg.cuenta_id = numero;
        reg.pipe_wr = pipe_wr;
        write(reg_fd, &reg, sizeof(reg));
    }

    close(reg_fd);
}

int main(void) {
    signal(SIGTERM, manejador_sigterm);
    signal(SIGINT,  manejador_sigterm);
    signal(SIGCHLD, SIG_DFL);

    if (leer_config("config.txt", &g_cfg) < 0) {
        fprintf(stderr, "No se pudo leer config.txt\n"); return 1;
    }

    sem_unlink(SEM_CUENTAS); sem_unlink(SEM_CONFIG);
    sem_t *sem_c = sem_open(SEM_CUENTAS, O_CREAT|O_EXCL, 0600, 1);
    sem_t *sem_g = sem_open(SEM_CONFIG,  O_CREAT|O_EXCL, 0600, 1);
    if (sem_c==SEM_FAILED || sem_g==SEM_FAILED) { perror("sem_open inicial"); return 1; }
    sem_close(sem_c); sem_close(sem_g);

    int reg_pipe[2];
    if (pipe(reg_pipe) < 0) {
        perror("pipe registro");
        return 1;
    }
    fcntl(reg_pipe[0], F_SETFL, O_NONBLOCK);

    int fd = open(g_cfg.archivo_cuentas, O_CREAT|O_RDWR, 0644);
    if (fd >= 0) close(fd);

    /* Crear las tres colas POSIX en modo no bloqueante para poder
     * drenarlas con mq_receive en un bucle sin quedarse colgado    */
    struct mq_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.mq_maxmsg = MQ_MAXMSG;
    attr.mq_flags  = O_NONBLOCK;

    mq_unlink(MQ_MONITOR); mq_unlink(MQ_LOG); mq_unlink(MQ_ALERTA);

    attr.mq_msgsize = sizeof(DatosMonitor);
    g_mq_monitor = mq_open(MQ_MONITOR, O_CREAT|O_RDWR|O_NONBLOCK, 0666, &attr);

    attr.mq_msgsize = sizeof(DatosLog);
    g_mq_log     = mq_open(MQ_LOG,     O_CREAT|O_RDWR|O_NONBLOCK, 0666, &attr);

    attr.mq_msgsize = sizeof(DatosAlerta);
    g_mq_alerta  = mq_open(MQ_ALERTA,  O_CREAT|O_RDWR|O_NONBLOCK, 0666, &attr);

    if (g_mq_monitor==-1 || g_mq_log==-1 || g_mq_alerta==-1) {
        perror("mq_open"); return 1;
    }

    lanzar_monitor();

    // Creamos socket TCP 1 vez fuera del bucle
    int server_fd;
    struct sockaddr_in address;
    int opt = 1;  // el valor que le pasas al socket 1 significa activado
    socklen_t addrlen = sizeof(address);

    if((server_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0){  // Creas el socket en memoria
        perror("Socket failed");
        return 1;
    }
    
    // Permitir reusar dirección y puerto si el servidor se reinicia
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {  // Configurar opciojnes antes de crearlo
        perror("setsockopt SO_REUSEADDR");
        close(server_fd);
        return -1;
    }

    address.sin_family = AF_INET;   //IpV4 
    address.sin_addr.s_addr = INADDR_ANY;   // Aceptar conexiones en cualquier interfaz
    address.sin_port = htons(g_cfg.puerto); // Puerto leído del config.txt

    // Asociar el socket a la IP y al puerto configurados
    if(bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0){
        perror("bind failed");
        return 1;
    }

    // Poner el socket en modo escucha, con cola de hasta 10 conexiones pendientes
    if(listen(server_fd, 10) < 0){
        perror("listen");
        return 1;
    }

    // Poner el socket en modo no bloqueante para poder usar poll()
    // Sin esto, accept() se bloquearía indefinidamente esperando una conexión,
    fcntl(server_fd, F_SETFL, O_NONBLOCK);

    printf("\n+==============================+\n");
    printf("|    SecureBank  --  Online    |\n");
    printf("|  Escuchando en puerto %-5d  |\n", g_cfg.puerto);
    printf("+==============================+\n\n");

    // Bucle principal del servidor: solo vigilamos el server_fd con poll.
    // pollfd es una estructura que le dice a poll() qué descriptor vigilar y qué evento esperar.
    struct pollfd pfds[2];
    pfds[0].fd = server_fd;     // El descriptor que queremos vigilar
    pfds[0].events = POLLIN;  // POLLIN = avisar cuando haya datos/conexión lista para leer
    pfds[1].fd = reg_pipe[0];
    pfds[1].events = POLLIN;

    while(!g_salir){
        int ret = poll(pfds, 2, 200);   // Esperar hasta 200ms; así podemos procesar colas y señales
        if (ret < 0) {
            if (errno == EINTR) continue;   // Interrupción por señal, reintentar
            perror("poll");
            break;
        }

        if (ret > 0 && (pfds[0].revents & POLLIN)) { // Hay una conexión entrante
            int new_socket = accept(server_fd, (struct sockaddr *)&address, &addrlen);
            if (new_socket < 0) {
                continue; // accept puede fallar si la conexión se cerró antes
            }

            pid_t pid_cliente = fork();
            if (pid_cliente < 0) {
                perror("fork cliente");
                close(new_socket);
                continue;
            }
            if (pid_cliente == 0) {
                close(server_fd);
                close(reg_pipe[0]);
                atender_cliente_socket(new_socket, reg_pipe[1]);
                exit(0);
            }

            close(new_socket);
        }

        if (ret > 0 && (pfds[1].revents & POLLIN)) {
            RegistroHijo reg;
            while (read(reg_pipe[0], &reg, sizeof(reg)) == sizeof(reg)) {
                if (g_num_hijos < MAX_HIJOS) {
                    g_hijos[g_num_hijos].pid = reg.pid_usuario;
                    g_hijos[g_num_hijos].cuenta_id = reg.cuenta_id;
                    g_hijos[g_num_hijos].pipe_wr = reg.pipe_wr;
                    g_num_hijos++;
                } else if (reg.pipe_wr != -1) {
                    close(reg.pipe_wr);
                }
            }
        }

        // Procesar colas (non-blocking)
        procesar_log();    // Drenar cola de logs pendientes de los hijos (no bloqueante)
        procesar_alertas();   // Drenar cola de alertas del monitor (no bloqueante)

        /// Recoger hijos que hayan terminado para evitar procesos zombie
        int wst;
        pid_t p;
        while ((p = waitpid(-1, &wst, WNOHANG)) > 0) {
            for (int i = 0; i < g_num_hijos; i++) {
                if (g_hijos[i].pid == p) {
                    if (g_hijos[i].pipe_wr != -1) close(g_hijos[i].pipe_wr);
                    g_hijos[i] = g_hijos[--g_num_hijos];  // Compactar la tabla
                    break;
                }
            }
        }
    }

    //---------------------------------------
    /* Cierre */
    g_salir = 1;

    for (int i = 0; i < g_num_hijos; i++) {
        if (g_hijos[i].pipe_wr != -1) close(g_hijos[i].pipe_wr);
        waitpid(g_hijos[i].pid, NULL, 0);
    }
    if (server_fd >= 0) close(server_fd);
    close(reg_pipe[0]);
    close(reg_pipe[1]);
    if (g_monitor_pid > 0) {
        kill(g_monitor_pid, SIGTERM);
        waitpid(g_monitor_pid, NULL, 0);
    }

    mq_close(g_mq_monitor); mq_unlink(MQ_MONITOR);
    mq_close(g_mq_log);     mq_unlink(MQ_LOG);
    mq_close(g_mq_alerta);  mq_unlink(MQ_ALERTA);
    sem_unlink(SEM_CUENTAS);
    sem_unlink(SEM_CONFIG);
    return 0;
}
