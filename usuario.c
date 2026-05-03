#include "banco_comun.h"

/* Variables globales del proceso hijo */
static mqd_t  g_mq_monitor = (mqd_t)-1;
static mqd_t  g_mq_log     = (mqd_t)-1;
static int    g_pipe_rd    = -1;
static int    g_cuenta_id  =  0;
static Config g_cfg;

/* Leer config (solo lectura) */
static void leer_config_usuario(void) {
    FILE *f = fopen("config.txt", "r");
    //
    // Completar
    //--------------------------------------------------------------------
    // zona de declaracion de varibales
    char linea[100];
    //Comprobamos que el puntero se ha creado correctamente
    if(f == NULL){
        perror("Error al abrir config.txt");
    }

    //Leemos linea a linea el archivo
    while (fgets(linea, sizeof(linea), f)) {
        if (linea[0] == '#' || strlen(linea) < 3) {
            continue;
        } else {
            if (strstr(linea, "PROXIMO_ID")) {
                sscanf(linea, "PROXIMO_ID=%d", &g_cfg.proximo_id);
            } else if (strstr(linea, "LIM_RET_EUR")) {
                sscanf(linea, "LIM_RET_EUR=%f", &g_cfg.lim_ret_eur);
            } else if (strstr(linea, "LIM_RET_USD")) {
                sscanf(linea, "LIM_RET_USD=%f", &g_cfg.lim_ret_usd);
            } else if (strstr(linea, "LIM_RET_GBP")) {
                sscanf(linea, "LIM_RET_GBP=%f", &g_cfg.lim_ret_gbp);
            } else if (strstr(linea, "LIM_TRF_EUR")) {
                sscanf(linea, "LIM_TRF_EUR=%f", &g_cfg.lim_trf_eur);
            } else if (strstr(linea, "LIM_TRF_USD")) {
                sscanf(linea, "LIM_TRF_USD=%f", &g_cfg.lim_trf_usd);
            } else if (strstr(linea, "LIM_TRF_GBP")) {
                sscanf(linea, "LIM_TRF_GBP=%f", &g_cfg.lim_trf_gbp);
            } else if (strstr(linea, "UMBRAL_RETIROS")) {
                sscanf(linea, "UMBRAL_RETIROS=%d", &g_cfg.umbral_retiros);
            } else if (strstr(linea, "UMBRAL_TRANSFERENCIAS")) {
                sscanf(linea, "UMBRAL_TRANSFERENCIAS=%d", &g_cfg.umbral_transferencias);
            } else if (strstr(linea, "NUM_HILOS")) {
                sscanf(linea, "NUM_HILOS=%d", &g_cfg.num_hilos);
            } else if (strstr(linea, "ARCHIVO_CUENTAS")) {
                sscanf(linea, "ARCHIVO_CUENTAS=%s", g_cfg.archivo_cuentas);
            } else if (strstr(linea, "ARCHIVO_LOG")) {
                sscanf(linea, "ARCHIVO_LOG=%s", g_cfg.archivo_log);
            } else if (strstr(linea, "CAMBIO_USD")) {
                sscanf(linea, "CAMBIO_USD=%f", &g_cfg.cambio_usd);
            } else if (strstr(linea, "CAMBIO_GBP")) {
                sscanf(linea, "CAMBIO_GBP=%f", &g_cfg.cambio_gbp);
            }
        }
    }

    //--------------------------------------------------------------------------

    fclose(f);
}

/* Semáforo: abrir con comprobación */
static sem_t *abrir_sem_cuentas(void) {
    sem_t *s = sem_open(SEM_CUENTAS, 0);
    if (s == SEM_FAILED) { perror("sem_open SEM_CUENTAS"); return NULL; }
    return s;
}

/*
E2C940E3C540D7C9C4C5D540D8E4C540C3D6D5E2C5D9E5C5E240C5D340C5E2D8E4C5D3C5E3D640C4C540C5E2E3C540D7D9D6C7D9C1D4C140C9D5E3C1C3E3D66B40D5D640C8C1C7C1E240C3C1E2D66B40D4D6C4C9C6C9C3C140E2C9C5D4D7D9C540C5D340E3C5E7E3D640C5D540C5C2C3C4C9C340E2E4D4C1D5C4D640F140C1D340C4C5C3C9D4D640C3D1D9C1C3E3C5D940C5D540C5C2C3C4C9C34BE2C940E3C540C4C9C3C5D540D8E4C540D7D6D940D8E4C540D3D640C8C1E240C3C1D4C2C9C1C4D640C4C940D8E4C540D5D640D3D640C8C1E240C3C1D4C2C9C1C4D64BD5D640C8C1C7C1E240D5C9D5C7D2D540C3D6C4C9C6C9C3C1C4D640C1D340C3D6C4C9C7D640D6C3E4D3E3D640C4C5D5E3D9D640C4C5D340C2D3D6D8E4C540C3D6C4C9C6C9C3C1C4D640C5D540C5C2C3C4C9C3
*/
/*  Leer cuenta por búsqueda lineal */
static int leer_cuenta(int id, Cuenta *c) {
    FILE *f = fopen(g_cfg.archivo_cuentas, "rb");
    if (!f) return -1;
    int ok = -1;
    while (fread(c, sizeof(Cuenta), 1, f) == 1)
        if (c->numero_cuenta == id) { ok = 0; break; }
    fclose(f);
    return ok;
}

/* Escribir cuenta por posición real en el fichero  */
static int escribir_cuenta(const Cuenta *c) {
    FILE *f = fopen(g_cfg.archivo_cuentas, "rb+");
    if (!f) return -1;
    Cuenta tmp;
    long pos = 0;
    int found = 0;
    while (fread(&tmp, sizeof(Cuenta), 1, f) == 1) {
        if (tmp.numero_cuenta == c->numero_cuenta) { found = 1; break; }
        pos += (long)sizeof(Cuenta);
    }
    if (!found) { fclose(f); return -1; }
    fseek(f, pos, SEEK_SET);
    int ok = (fwrite(c, sizeof(Cuenta), 1, f) == 1) ? 0 : -1;
    fflush(f);
    fclose(f);
    return ok;
}

/*
E2C940E3C540D7C9C4C5D540D8E4C540C3D6D5E2C5D9E5C5E240C5D340C5E2D8E4C5D3C5E3D640C4C540C5E2E3C540D7D9D6C7D9C1D4C140C9D5E3C1C3E3D66B40D5D640C8C1C7C1E240C3C1E2D66B40D4D6C4C9C6C9C3C140E2C9C5D4D7D9C540C5D340E3C5E7E3D640C5D540C5C2C3C4C9C340E2E4D4C1D5C4D640F140C1D340C4C5C3C9D4D640C3D1D9C1C3E3C5D940C5D540C5C2C3C4C9C34BE2C940E3C540C4C9C3C5D540D8E4C540D7D6D940D8E4C540D3D640C8C1E240C3C1D4C2C9C1C4D640C4C940D8E4C540D5D640D3D640C8C1E240C3C1D4C2C9C1C4D64BD5D640C8C1C7C1E240D5C9D5C7D2D540C3D6C4C9C6C9C3C1C4D640C1D340C3D6C4C9C7D640D6C3E4D3E3D640C4C5D5E3D9D640C4C5D340C2D3D6D8E4C540C3D6C4C9C6C9C3C1C4D640C5D540C5C2C3C4C9C3
*/
/* Enviar mensajes a las colas POSIX */
static void enviar_monitor(int cuenta_origen, int cuenta_destino,
                           int tipo_op, float cantidad, int divisa) {
    DatosMonitor dm;
    memset(&dm, 0, sizeof(dm));
    //
    // Completar
    dm.cuenta_origen = cuenta_origen;
    dm.cuenta_destino = cuenta_destino;
    dm.tipo_op = tipo_op;
    dm.cantidad = cantidad;
    dm.divisa = divisa;
    timestamp_ahora(dm.timestamp, sizeof(dm.timestamp));
    //
    mq_send(g_mq_monitor, (const char *)&dm, sizeof(dm), 0);
}

static void enviar_log(int tipo_op, float cantidad, int divisa, int estado) {
    DatosLog dl;
    memset(&dl, 0, sizeof(dl));
    //
    // Completar
    dl.cuenta_id = g_cuenta_id;
    dl.tipo_op = tipo_op;
    dl.pid_hijo = getpid();
    dl.cantidad = cantidad;
    dl.divisa = divisa;
    dl.estado = estado;
    timestamp_ahora(dl.timestamp, sizeof(dl.timestamp));
    mq_send(g_mq_log, (const char*)&dl, sizeof(dl), 0);
    //
}

/*
E2C940E3C540D7C9C4C5D540D8E4C540C3D6D5E2C5D9E5C5E240C5D340C5E2D8E4C5D3C5E3D640C4C540C5E2E3C540D7D9D6C7D9C1D4C140C9D5E3C1C3E3D66B40D5D640C8C1C7C1E240C3C1E2D66B40D4D6C4C9C6C9C3C140E2C9C5D4D7D9C540C5D340E3C5E7E3D640C5D540C5C2C3C4C9C340E2E4D4C1D5C4D640F140C1D340C4C5C3C9D4D640C3D1D9C1C3E3C5D940C5D540C5C2C3C4C9C34BE2C940E3C540C4C9C3C5D540D8E4C540D7D6D940D8E4C540D3D640C8C1E240C3C1D4C2C9C1C4D640C4C940D8E4C540D5D640D3D640C8C1E240C3C1D4C2C9C1C4D64BD5D640C8C1C7C1E240D5C9D5C7D2D540C3D6C4C9C6C9C3C1C4D640C1D340C3D6C4C9C7D640D6C3E4D3E3D640C4C5D5E3D9D640C4C5D340C2D3D6D8E4C540C3D6C4C9C6C9C3C1C4D640C5D540C5C2C3C4C9C3
*/
/* ════════════════════════════════════════════════════════════
 * Threads de operación bancaria
 * ════════════════════════════════════════════════════════════ */
typedef struct {
    int   tipo_op;
    int   cuenta_id;
    int   cuenta_destino;
    float cantidad;
    int   divisa_origen;
    int   divisa_destino;
} DatosOperacion;

/* Depósito  */
static void *thread_deposito(void *arg) {
    // Abrimos el semáforo
    sem_t *sem = abrir_sem_cuentas();
    
    // Convertimos el argumento genérico void* al tipo real DatosOperacion
    //porque POSIX obliga a que todos los threads reciban void* 
    DatosOperacion *d = (DatosOperacion *)arg;

    // si el semaforo no esta disponible lanzamos un error
    if(!sem){
        free(d);
        return NULL;
    }

    // Bloqueamos el semaforo y esperamos en caso de que haya otro thread con el semaforo
    sem_wait(sem);

    // Leemos los datos de la cuenta del fichero binario
    Cuenta c;
    if(leer_cuenta(d->cuenta_id, &c) != 0){
        //Como la cuenta no existe liberamos el semaforo
        sem_post(sem);
        // nos desvinculamos del semaforo
        sem_close(sem);
        // Liberamos la memoria del struct
        free(d);
        // terminamos el thread
        return NULL;
    }

    // Validar que la cantidad sea positiva
    if (d->cantidad <= 0) {
        printf("Error: la cantidad a depositar debe ser positiva.\n");
        enviar_log(OP_DEPOSITO, d->cantidad, d->divisa_origen, 0);
        sem_post(sem);
        sem_close(sem);
        free(d);
        return NULL;
    }
    
    // Sumamos la cantidad que el usuario quiere ingresar al saldo de la divisa elegida
    if(d->divisa_origen == DIV_EUR){
        c.saldo_eur += d->cantidad;
    }else if(d->divisa_origen == DIV_USD){
        c.saldo_usd += d->cantidad;
    }else if(d->divisa_origen == DIV_GBP){
        c.saldo_gbp += d->cantidad;
    }

    // guardamos la cuenta modificada en el fichero binario de cuentas.dat
    escribir_cuenta(&c);

    // Liberamos el semaforo para que tenga acceso el siguiente thread (+1)
    sem_post(sem);

    // enviamos al monitor los datos de la operación
    enviar_monitor(d->cuenta_id, 0, OP_DEPOSITO, d->cantidad, d->divisa_origen);

    // enviamos al log la operacion, estado=1, que significa éxito
    enviar_log(OP_DEPOSITO, d->cantidad, d->divisa_origen, 1);

    // Liberamos la memoria del struct DatosOperacion reservada en procesar_opcion
    free(d);

    // Devolvemos NULL porque los threads POSIX siempre devuelven void*
    return NULL;
}

/* Retiro */
static void *thread_retiro(void *arg) {

    // Abrimos el semáforo
    sem_t *sem = abrir_sem_cuentas();
    
    // Convertimos el argumento genérico void* al tipo real DatosOperacion
    //porque POSIX obliga a que todos los threads reciban void* 
    DatosOperacion *d = (DatosOperacion *)arg;

    // si el semaforo no esta disponible lanzamos un error
    if(!sem){
        free(d);
        return NULL;
    }

    // Bloqueamos el semaforo y esperamos en caso de que haya otro thread con el semaforo
    sem_wait(sem);

    // Leemos los datos de la cuenta del fichero binario
    Cuenta c;

    // Buscamos la cuenta en el fichero binario
    if(leer_cuenta(d->cuenta_id, &c) != 0){
        //Como la cuenta no existe liberamos el semaforo
        sem_post(sem);
        // nos desvinculamos del semaforo
        sem_close(sem);
        // Liberamos la memoria del struct
        free(d);
        // terminamos el thread
        return NULL;
    }
    
    // obtenemos el saldo actual y el limite de retiro
    float saldo_actual;
    float limite;

    // Validar que la cantidad sea positiva (no se permiten retiros negativos ni cero)
    if (d->cantidad <= 0) {
        printf("Error: la cantidad a retirar debe ser positiva.\n");
        enviar_log(OP_RETIRO, d->cantidad, d->divisa_origen, 0);
        sem_post(sem);
        sem_close(sem);
        free(d);
        return NULL;
    }   

    if(d->divisa_origen == DIV_EUR){
        saldo_actual = c.saldo_eur;
        limite = g_cfg.lim_ret_eur;
    }else if(d->divisa_origen == DIV_USD){
        saldo_actual = c.saldo_usd;
        limite = g_cfg.lim_ret_usd;
    }else if(d->divisa_origen == DIV_GBP){
        saldo_actual = c.saldo_gbp;
        limite = g_cfg.lim_ret_gbp;
    }else{
        // Divisa no reconocida, abortamos
        sem_post(sem);
        sem_close(sem);
        free(d);
        return NULL;
    }

    // comprobamos que el usuario tenga suficiente dinero para hacer el retiro
    if(d->cantidad > saldo_actual) {
        printf("Saldo insuficiente. Saldo actual: %.2f\n", saldo_actual);
        // Notificamos al log con estado=0, operacion fallida
        enviar_log(OP_RETIRO, d->cantidad, d->divisa_origen, 0);
        // Liberamos el semaforo para que otro thread pueda cogerlo (+1)
        sem_post(sem);
        // Nos desvinculamos del semaforo
        sem_close(sem);
        // liberamos el puntero que contiene los datos de la operación
        free(d);
        return NULL;
    }

    // Verificamos que la cantidad no supera el límite de retiro
    if (d->cantidad > limite) {
        printf("Cantidad supera el límite permitido de retiro: %.2f\n", limite);
        // Notificamos al log con estado=0 (operación fallida)
        enviar_log(OP_RETIRO, d->cantidad, d->divisa_origen, 0);
        // Liberamos el semaforo para que otro thread pueda cogerlo (+1)
        sem_post(sem);
        // Nos desvinculamos del semaforo
        sem_close(sem);
        // liberamos el puntero que contiene los datos de la operación
        free(d);
        return NULL;
    }

    // Modificamos el saldo del usuario
    if (d->divisa_origen == DIV_EUR) {
        c.saldo_eur -= d->cantidad; 
    } else if (d->divisa_origen == DIV_USD) {
        c.saldo_usd -= d->cantidad; 
    } else if (d->divisa_origen == DIV_GBP) {
        c.saldo_gbp -= d->cantidad; 
    }

    // guardamos la cuenta modificada en el fichero binario de cuentas.dat
    escribir_cuenta(&c);

    // Liberamos el semaforo para que tenga acceso el siguiente thread (+1)
    sem_post(sem);

    // enviamos al monitor los datos de la operación
    enviar_monitor(d->cuenta_id, 0, OP_RETIRO, d->cantidad, d->divisa_origen);

    // enviamos al log la operacion, estado=1, que significa éxito
    enviar_log(OP_RETIRO, d->cantidad, d->divisa_origen, 1);

    // Liberamos la memoria del struct DatosOperacion reservada en procesar_opcion
    free(d);

    // Devolvemos NULL porque los threads POSIX siempre devuelven void*
    return NULL;
    //
}

/*
E2C940E3C540D7C9C4C5D540D8E4C540C3D6D5E2C5D9E5C5E240C5D340C5E2D8E4C5D3C5E3D640C4C540C5E2E3C540D7D9D6C7D9C1D4C140C9D5E3C1C3E3D66B40D5D640C8C1C7C1E240C3C1E2D66B40D4D6C4C9C6C9C3C140E2C9C5D4D7D9C540C5D340E3C5E7E3D640C5D540C5C2C3C4C9C340E2E4D4C1D5C4D640F140C1D340C4C5C3C9D4D640C3D1D9C1C3E3C5D940C5D540C5C2C3C4C9C34BE2C940E3C540C4C9C3C5D540D8E4C540D7D6D940D8E4C540D3D640C8C1E240C3C1D4C2C9C1C4D640C4C940D8E4C540D5D640D3D640C8C1E240C3C1D4C2C9C1C4D64BD5D640C8C1C7C1E240D5C9D5C7D2D540C3D6C4C9C6C9C3C1C4D640C1D340C3D6C4C9C7D640D6C3E4D3E3D640C4C5D5E3D9D640C4C5D340C2D3D6D8E4C540C3D6C4C9C6C9C3C1C4D640C5D540C5C2C3C4C9C3
*/
/* Transferencia */
static void *thread_transferencia(void *arg) {
    // Abrimos el semáforo
    sem_t *sem = abrir_sem_cuentas();

    // Convertimos el argumento genérico void* al tipo real DatosOperacion
    DatosOperacion *d = (DatosOperacion *)arg;

    // Si el semáforo no está disponible, abortamos
    if (!sem) {
        free(d);
        return NULL;
    }

    // Bloqueamos el semáforo (exclusión mutua sobre cuentas.dat)
    sem_wait(sem);

    // Leemos la cuenta ORIGEN del fichero binario
    Cuenta origen;
    
    // Evitar transferencia a la misma cuenta
    if (d->cuenta_id == d->cuenta_destino) {
        printf("Error: no se puede transferir a la misma cuenta origen.\n");
        enviar_log(OP_TRANSFERENCIA, d->cantidad, d->divisa_origen, 0);
        sem_post(sem);
        sem_close(sem);
        free(d);
        return NULL;
    }

    if (leer_cuenta(d->cuenta_id, &origen) != 0) {
        sem_post(sem);
        sem_close(sem);
        free(d);
        return NULL;
    }

    // Leemos la cuenta DESTINO del fichero binario
    Cuenta destino;
    if (leer_cuenta(d->cuenta_destino, &destino) != 0) {
        printf("Cuenta destino %d no encontrada.\n", d->cuenta_destino);
        enviar_log(OP_TRANSFERENCIA, d->cantidad, d->divisa_origen, 0);
        sem_post(sem);
        sem_close(sem);
        free(d);
        return NULL;
    }

    // Obtenemos saldo actual y límite de transferencia según la divisa
    float saldo_actual;
    float limite;

    if (d->divisa_origen == DIV_EUR) {
        saldo_actual = origen.saldo_eur;
        limite       = g_cfg.lim_trf_eur;
    } else if (d->divisa_origen == DIV_USD) {
        saldo_actual = origen.saldo_usd;
        limite       = g_cfg.lim_trf_usd;
    } else if (d->divisa_origen == DIV_GBP) {
        saldo_actual = origen.saldo_gbp;
        limite       = g_cfg.lim_trf_gbp;
    } else {
        // Divisa no reconocida, abortamos
        sem_post(sem);
        sem_close(sem);
        free(d);
        return NULL;
    }

    // Validar que la cantidad sea positiva
    if (d->cantidad <= 0) {
        printf("Error: la cantidad de transferencia debe ser positiva.\n");
        enviar_log(OP_TRANSFERENCIA, d->cantidad, d->divisa_origen, 0);
        sem_post(sem);
        sem_close(sem);
        free(d);
        return NULL;
    }

    // Comprobamos saldo suficiente
    if (d->cantidad > saldo_actual) {
        printf("Saldo insuficiente para la transferencia. Saldo actual: %.2f\n", saldo_actual);
        enviar_log(OP_TRANSFERENCIA, d->cantidad, d->divisa_origen, 0);
        sem_post(sem); //liberamos el semaforo(+1)
        sem_close(sem);
        free(d);
        return NULL;
    }

    // Comprobamos que no supera el límite de transferencia
    if (d->cantidad > limite) {
        printf("Cantidad supera el límite permitido de transferencia: %.2f\n", limite);
        enviar_log(OP_TRANSFERENCIA, d->cantidad, d->divisa_origen, 0);
        sem_post(sem); //liberamos el semaforo(+1)
        sem_close(sem);
        free(d);
        return NULL;
    }

    // Descontamos de la cuenta origen
    if (d->divisa_origen == DIV_EUR) {
        origen.saldo_eur -= d->cantidad;
        destino.saldo_eur += d->cantidad;
    } else if (d->divisa_origen == DIV_USD) {
        origen.saldo_usd -= d->cantidad;
        destino.saldo_usd += d->cantidad;
    } else if (d->divisa_origen == DIV_GBP) {
        origen.saldo_gbp -= d->cantidad;
        destino.saldo_gbp += d->cantidad;
    }

    // Guardamos ambas cuentas modificadas en el fichero binario
    escribir_cuenta(&origen);
    escribir_cuenta(&destino);

    // Liberamos el semáforo (fin de sección crítica)
    sem_post(sem);
    sem_close(sem);

    // Notificamos al monitor con origen Y destino (para detección de transferencias repetitivas)
    enviar_monitor(d->cuenta_id, d->cuenta_destino, OP_TRANSFERENCIA, d->cantidad, d->divisa_origen);

    // Notificamos al log con estado=1 (éxito)
    enviar_log(OP_TRANSFERENCIA, d->cantidad, d->divisa_origen, 1);

    // Liberamos la memoria del struct
    free(d);

    return NULL;
}

/* Mover divisas */
static void *thread_mover_divisa(void *arg) {
    // Abrimos el semáforo
    sem_t *sem = abrir_sem_cuentas();

    // Convertimos el argumento genérico void* al tipo real DatosOperacion
    DatosOperacion *d = (DatosOperacion *)arg;

    // Si el semáforo no está disponible, abortamos
    if (!sem) {
        free(d);
        return NULL;
    }

    // No tiene sentido mover dinero a la misma divisa
    if (d->divisa_origen == d->divisa_destino) {
        printf("La divisa origen y destino son iguales.\n");
        free(d);
        return NULL;
    }

    // Bloqueamos el semáforo (exclusión mutua sobre cuentas.dat)
    sem_wait(sem);

    // Leemos la cuenta del fichero binario
    Cuenta c;
    if (leer_cuenta(d->cuenta_id, &c) != 0) {
        sem_post(sem);
        sem_close(sem);
        free(d);
        return NULL;
    }

    // Obtenemos el saldo de la divisa origen
    float saldo_actual;
    if (d->divisa_origen == DIV_EUR)      saldo_actual = c.saldo_eur;
    else if (d->divisa_origen == DIV_USD) saldo_actual = c.saldo_usd;
    else if (d->divisa_origen == DIV_GBP) saldo_actual = c.saldo_gbp;
    else {
        sem_post(sem);
        sem_close(sem);
        free(d);
        return NULL;
    }

    // Validar que la cantidad sea positiva
    if (d->cantidad <= 0) {
        printf("Error: la cantidad a convertir debe ser positiva.\n");
        sem_post(sem);
        sem_close(sem);
        free(d);
        return NULL;
    }

    // Comprobamos que hay saldo suficiente en la divisa origen
    if (d->cantidad > saldo_actual) {
        printf("Saldo insuficiente en %s. Saldo actual: %.2f\n",
               nombre_divisa(d->divisa_origen), saldo_actual);
        enviar_log(OP_MOVER_DIVISA, d->cantidad, d->divisa_origen, 0);
        sem_post(sem);
        sem_close(sem);
        free(d);
        return NULL;
    }

    // Calculamos la cantidad equivalente en la divisa destino.
    // Primero convertimos a EUR (moneda base), luego a la divisa destino.
    float cantidad_eur;
    if (d->divisa_origen == DIV_EUR)      cantidad_eur = d->cantidad;
    else if (d->divisa_origen == DIV_USD) cantidad_eur = d->cantidad / g_cfg.cambio_usd;
    else                                  cantidad_eur = d->cantidad / g_cfg.cambio_gbp;

    float cantidad_destino;
    if (d->divisa_destino == DIV_EUR)      cantidad_destino = cantidad_eur;
    else if (d->divisa_destino == DIV_USD) cantidad_destino = cantidad_eur * g_cfg.cambio_usd;
    else                                   cantidad_destino = cantidad_eur * g_cfg.cambio_gbp;

    // Restamos de la divisa origen
    if (d->divisa_origen == DIV_EUR)      c.saldo_eur -= d->cantidad;
    else if (d->divisa_origen == DIV_USD) c.saldo_usd -= d->cantidad;
    else                                  c.saldo_gbp -= d->cantidad;

    // Sumamos a la divisa destino
    if (d->divisa_destino == DIV_EUR)      c.saldo_eur += cantidad_destino;
    else if (d->divisa_destino == DIV_USD) c.saldo_usd += cantidad_destino;
    else                                   c.saldo_gbp += cantidad_destino;

    // Guardamos la cuenta modificada en el fichero binario
    escribir_cuenta(&c);

    // Liberamos el semáforo (fin de sección crítica)
    sem_post(sem);
    sem_close(sem);

    // Informamos al monitor (origen y destino a 0, no es transferencia entre cuentas)
    enviar_monitor(d->cuenta_id, 0, OP_MOVER_DIVISA, d->cantidad, d->divisa_origen);

    // Notificamos al log con estado=1 (éxito)
    enviar_log(OP_MOVER_DIVISA, d->cantidad, d->divisa_origen, 1);

    // Liberamos la memoria del struct
    free(d);

    return NULL;
}

/* Lanzar thread */
static void lanzar_operacion(void *(*fn)(void*), DatosOperacion *d) {
    pthread_t t;
    pthread_create(&t, NULL, fn, d);
    pthread_join(t, NULL);
}

/* Consultar saldos */
static void consultar_saldos(void) {
    // Abrimos el semáforo (solo lectura, pero igualmente necesitamos exclusión mutua)
    sem_t *sem = abrir_sem_cuentas();
    if (!sem) return;

    // Bloqueamos el semáforo
    sem_wait(sem);

    // Leemos la cuenta del fichero binario
    Cuenta c;
    if (leer_cuenta(g_cuenta_id, &c) != 0) {
        printf("Error al leer la cuenta.\n");
        sem_post(sem);
        sem_close(sem);
        return;
    }

    // Liberamos el semáforo (ya tenemos los datos en memoria)
    sem_post(sem);
    sem_close(sem);

    // Calculamos el total convertido a EUR usando los tipos de cambio del config
    float total_eur = c.saldo_eur
                    + (c.saldo_usd / g_cfg.cambio_usd)
                    + (c.saldo_gbp / g_cfg.cambio_gbp);

    // Mostramos el desglose por divisa y el total en EUR
    printf("\n+------------------------------+\n");
    printf("|  Saldos de la cuenta %-7d |\n", g_cuenta_id);
    printf("|------------------------------|\n");
    printf("|  EUR: %20.2f €  |\n", c.saldo_eur);
    printf("|  USD: %20.2f $  |\n", c.saldo_usd);
    printf("|  GBP: %20.2f £  |\n", c.saldo_gbp);
    printf("|------------------------------|\n");
    printf("|  TOTAL (EUR): %13.2f €  |\n", total_eur);
    printf("+------------------------------+\n");
}

/* Pedir divisa */
static int pedir_divisa(const char *prompt) {
    int d = -1;
    while (d < 0 || d > 2) {
        printf("%s (0=EUR, 1=USD, 2=GBP): ", prompt);
        fflush(stdout);
        if (scanf("%d", &d) != 1) {
            int c; while ((c=getchar())!='\n'&&c!=EOF);
            d = -1;
        }
    }
    return d;
}

/* Procesar opción del menú */
static void procesar_opcion(int opcion) {
    DatosOperacion *d = NULL;
    switch (opcion) {
    case 1:
        d = calloc(1, sizeof(DatosOperacion));
        d->tipo_op       = OP_DEPOSITO;
        d->cuenta_id     = g_cuenta_id;
        d->divisa_origen = pedir_divisa("Divisa");
        printf("Cantidad a depositar: "); fflush(stdout);
        scanf("%f", &d->cantidad);
        lanzar_operacion(thread_deposito, d);
        break;
    case 2:
        d = calloc(1, sizeof(DatosOperacion));
        d->tipo_op       = OP_RETIRO;
        d->cuenta_id     = g_cuenta_id;
        d->divisa_origen = pedir_divisa("Divisa");
        printf("Cantidad a retirar: "); fflush(stdout);
        scanf("%f", &d->cantidad);
        lanzar_operacion(thread_retiro, d);
        break;
    case 3:
        d = calloc(1, sizeof(DatosOperacion));
        d->tipo_op       = OP_TRANSFERENCIA;
        d->cuenta_id     = g_cuenta_id;
        printf("Cuenta destino: "); fflush(stdout);
        scanf("%d", &d->cuenta_destino);
        d->divisa_origen = pedir_divisa("Divisa");
        printf("Cantidad: "); fflush(stdout);
        scanf("%f", &d->cantidad);
        lanzar_operacion(thread_transferencia, d);
        break;
    case 4:
        consultar_saldos();
        break;
    case 5:
        d = calloc(1, sizeof(DatosOperacion));
        d->tipo_op        = OP_MOVER_DIVISA;
        d->cuenta_id      = g_cuenta_id;
        d->divisa_origen  = pedir_divisa("Divisa origen");
        d->divisa_destino = pedir_divisa("Divisa destino");
        printf("Cantidad a convertir: "); fflush(stdout);
        scanf("%f", &d->cantidad);
        lanzar_operacion(thread_mover_divisa, d);
        break;
    case 6:
        printf("Saliendo...\n");
        mq_close(g_mq_monitor);
        mq_close(g_mq_log);
        exit(0);
    default:
        printf("Opcion no valida.\n");
    }
}

/* Mostrar menú */
static void mostrar_menu(void) {
    printf("\n+==========================+\n");
    printf("|  Cuenta %-17d|\n", g_cuenta_id);
    printf("|==========================|\n");
    printf("| 1. Deposito              |\n");
    printf("| 2. Retiro                |\n");
    printf("| 3. Transferencia         |\n");
    printf("| 4. Consultar saldos      |\n");
    printf("| 5. Mover divisas         |\n");
    printf("| 6. Salir                 |\n");
    printf("+==========================+\n");
    printf("Opcion: ");
    fflush(stdout);
}

/*
E2C940E3C540D7C9C4C5D540D8E4C540C3D6D5E2C5D9E5C5E240C5D340C5E2D8E4C5D3C5E3D640C4C540C5E2E3C540D7D9D6C7D9C1D4C140C9D5E3C1C3E3D66B40D5D640C8C1C7C1E240C3C1E2D66B40D4D6C4C9C6C9C3C140E2C9C5D4D7D9C540C5D340E3C5E7E3D640C5D540C5C2C3C4C9C340E2E4D4C1D5C4D640F140C1D340C4C5C3C9D4D640C3D1D9C1C3E3C5D940C5D540C5C2C3C4C9C34BE2C940E3C540C4C9C3C5D540D8E4C540D7D6D940D8E4C540D3D640C8C1E240C3C1D4C2C9C1C4D640C4C940D8E4C540D5D640D3D640C8C1E240C3C1D4C2C9C1C4D64BD5D640C8C1C7C1E240D5C9D5C7D2D540C3D6C4C9C6C9C3C1C4D640C1D340C3D6C4C9C7D640D6C3E4D3E3D640C4C5D5E3D9D640C4C5D340C2D3D6D8E4C540C3D6C4C9C6C9C3C1C4D640C5D540C5C2C3C4C9C3
*/
int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Uso: usuario <cuenta_id> <pipe_rd>\n");
        return 1;
    }
    g_cuenta_id = atoi(argv[1]);
    g_pipe_rd   = atoi(argv[2]);

    leer_config_usuario();

    /* Abrir las colas POSIX ya creadas por banco.c */
    g_mq_monitor = mq_open(MQ_MONITOR, O_WRONLY);
    g_mq_log     = mq_open(MQ_LOG,     O_WRONLY);
    if (g_mq_monitor==(mqd_t)-1 || g_mq_log==(mqd_t)-1) {
        perror("mq_open en usuario");
        return 1;
    }

    sem_t *test = sem_open(SEM_CUENTAS, 0);
    if (test == SEM_FAILED) {
        fprintf(stderr, "[Cuenta %d] ERROR: semaforo %s no disponible: %s\n",
                g_cuenta_id, SEM_CUENTAS, strerror(errno));
        return 1;
    }
    sem_close(test);

    struct pollfd fds[2];
    fds[0].fd = STDIN_FILENO; fds[0].events = POLLIN;
    fds[1].fd = g_pipe_rd;    fds[1].events = POLLIN;

    mostrar_menu();

    while (1) {
        int ret = poll(fds, 2, -1);
        if (ret < 0) { if (errno==EINTR) continue; break; }

        if (fds[1].revents & POLLIN) {
            char buf[256];
            ssize_t n = read(g_pipe_rd, buf, sizeof(buf)-1);
            if (n > 0) {
                buf[n] = '\0';
                printf("\n[ALERTA DEL BANCO]: %s\n", buf);
                fflush(stdout);
                if (strstr(buf, "BLOQUEO")) {
                    mq_close(g_mq_monitor);
                    mq_close(g_mq_log);
                    close(g_pipe_rd);
                    exit(0);
                }
            }
        }

        if (fds[0].revents & POLLIN) {
            int opcion;
            if (scanf("%d", &opcion) == 1) {
                procesar_opcion(opcion);
            } else {
                int c; while ((c=getchar())!='\n'&&c!=EOF);
            }
            mostrar_menu();
        }
    }

    mq_close(g_mq_monitor);
    mq_close(g_mq_log);
    close(g_pipe_rd);
    return 0;
}
