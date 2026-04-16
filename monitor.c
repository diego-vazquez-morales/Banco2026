#include "banco_comun.h"

/* Parámetros de detección */
static int   g_umbral_retiros        = 3;
static int   g_umbral_transferencias = 5;
static volatile sig_atomic_t g_salir = 0;

static mqd_t g_mq_monitor = (mqd_t)-1;
static mqd_t g_mq_alerta  = (mqd_t)-1;

/* Señal de terminación  */
static void manejador_sigterm(int s) { (void)s; g_salir = 1; }

/* Leer config */
static void leer_config_monitor(void) {
   //
   //
   //
    FILE *f = fopen("config.txt", "r");
    if (!f) return;

    //Preparar el buffer
    char linea[256];

    while (fgets(linea, sizeof(linea), f)) {
        char clave[64], valor[64];
        if (sscanf(linea, "%63[^=]=%63s", clave, valor) != 2) continue; //lee hasta encintrar un =

        if (strcmp(clave, "UMBRAL_RETIROS") == 0) // compara clave == umbral_retiros(iguales)
            g_umbral_retiros = atoi(valor);
        else if (strcmp(clave, "UMBRAL_TRANSFERENCIAS") == 0)
            g_umbral_transferencias = atoi(valor);
    }

    fclose(f);
    //
    //
    //
}

/*  Enviar alerta al padre  */
static void enviar_alerta(int cuenta_id, const char *tipo_alerta) { //alerta al banco sobre anomalía
    //
    // 
    //
    DatosAlerta da;
    da.cuenta_id = cuenta_id; //asocia la alerta a una cuenta concreta
    //copia el texto de la alerta en la estructura
    snprintf(da.mensaje, sizeof(da.mensaje), "%s", tipo_alerta); //controla tamaño del buffer

    //cola que conecta monitor con banco
    mq_send(g_mq_alerta, (const char *)&da, sizeof(da), 0); //estructura a bytes para enviarla
    //
    //
    //
}


#define MAX_CUENTAS_TRACK 256
#define MAX_TRAN_TRACK    64

typedef struct {
    int   cuenta_id;
    int   retiros_consecutivos;
    float ultimo_retiro;
} TrackRetiros;

typedef struct {
    int cuenta_origen;
    int cuenta_destino;
    int contador;
} TrackTransferencias;

static TrackRetiros        g_retiros[MAX_CUENTAS_TRACK];
static int                 g_n_retiros = 0;
static TrackTransferencias g_transf[MAX_TRAN_TRACK];
static int                 g_n_transf = 0;

//Analizar operación bancaria
/*  Analizar mensaje  */
static void analizar(const DatosMonitor *dm) {

    /*  Retiros consecutivos */
    if (dm->tipo_op == OP_RETIRO) { 
    //
    // 
    //
    int idx = -1; //indice (guarda posicion)
    for (int i = 0; i < g_n_retiros; i++) {
        if (g_retiros[i].cuenta_id == dm->cuenta_origen) {
            idx = i; //si encuentra la cuenta se guarda su posicion en idx (sino -1)
            break;
        }
}

    //si no existe crea registro
    if (idx < 0 && g_n_retiros < MAX_CUENTAS_TRACK) {  //la cuenta no estaba y hay espacios
        idx = g_n_retiros++;
        g_retiros[idx].cuenta_id = dm->cuenta_origen; //cuenta al array
        g_retiros[idx].retiros_consecutivos = 0; //inicializa contador
    }

    
    if (idx >= 0) {
        g_retiros[idx].retiros_consecutivos++; //se suma 1 cada vez que hay un retiro

        if (g_retiros[idx].retiros_consecutivos >= g_umbral_retiros) { //si retiros>umbral alerta
            enviar_alerta(dm->cuenta_origen, ALERTA_RETIROS);
            g_retiros[idx].retiros_consecutivos = 0; //reseter contador
        }
    }
    //
    //
    //
    } else {
        for (int i = 0; i < g_n_retiros; i++)
            if (g_retiros[i].cuenta_id == dm->cuenta_origen) {
                g_retiros[i].retiros_consecutivos = 0; break;
            }
    }

    /*  Transferencias repetitivas */
    if (dm->tipo_op == OP_TRANSFERENCIA) {
        int idx = -1;
        for (int i = 0; i < g_n_transf; i++)
            if (g_transf[i].cuenta_origen  == dm->cuenta_origen &&
                g_transf[i].cuenta_destino == dm->cuenta_destino) { idx = i; break; }
        if (idx < 0 && g_n_transf < MAX_TRAN_TRACK) {
            idx = g_n_transf++;
            g_transf[idx].cuenta_origen  = dm->cuenta_origen;
            g_transf[idx].cuenta_destino = dm->cuenta_destino;
            g_transf[idx].contador       = 0;
        }
        if (idx >= 0) {
            g_transf[idx].contador++;
            if (g_transf[idx].contador >= g_umbral_transferencias) {
                enviar_alerta(dm->cuenta_origen, ALERTA_TRANSFERENCIAS);
                g_transf[idx].contador = 0;
            }
        }
    }
}


int main(void) {
    signal(SIGTERM, manejador_sigterm);
    signal(SIGINT,  SIG_IGN);

    leer_config_monitor();

    /* Abrir las colas ya creadas por banco.c */
    g_mq_monitor = mq_open(MQ_MONITOR, O_RDONLY);
    g_mq_alerta  = mq_open(MQ_ALERTA,  O_WRONLY);
    if (g_mq_monitor==(mqd_t)-1 || g_mq_alerta==(mqd_t)-1) {
        perror("[MONITOR] mq_open");
        return 1;
    }

    /* poll() sobre el descriptor de MQ_MONITOR: bloqueo eficiente */
    struct pollfd pfd;
    pfd.fd     = (int)g_mq_monitor; //vigilar: cola de mensaje donde llegan las operaciones
    pfd.events = POLLIN; //saber cuando hay datos para leer

    while (!g_salir) { //hasta que se le dice que pare
        //
        // 
        //
        // (descriptor)
        int ret = poll(&pfd, 1, 500); //esperar hasta un mensaje o tras 500ms
        if (ret > 0 && (pfd.revents & POLLIN)) { //si hay datos para leer
            //lee mensaje de la cola MQ_MONITOR
            DatosMonitor dm;
            if (mq_receive(g_mq_monitor, (char *)&dm, sizeof(dm), NULL) > 0) { 
                analizar(&dm);
            }
    
        //
        //
        //
        }
    }
    mq_close(g_mq_monitor);
    mq_close(g_mq_alerta);
    return 0;
}
