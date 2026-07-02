#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
//#include <dbFldTypes.h>
//#include <db_access.h>
#include <dbChannel.h>
#include <dbCommon.h>
#include <dbLock.h>
#include <dbEvent.h>
#include <dbExtractArray.h>
#include <caeventmask.h>
#include <epicsEvent.h>
#include <epicsExport.h>
#include <epicsMutex.h>
#include <epicsString.h>
#include <epicsThread.h>
#include <epicsTime.h>
#include <iocsh.h>

/*
 Private copy of one compress-record VAL update.
 After the callback creates a Snapshot, worker threads can process it
 without accessing the EPICS record memory directly.
 */
typedef struct Snapshot {
    struct Snapshot *next;
    long count;
    epicsTimeStamp stamp;
    double values[1];
} Snapshot;

/*
 Shared runtime context.
 head/tail form a producer-consumer queue shared between eventCallback()
 and dispatcherThread(); access is protected by lock.
 */
typedef struct ThreadExampleContext {
    dbChannel *valueChan;
    dbEventCtx eventCtx;
    dbEventSubscription subscription;
    epicsEventId wakeup;
    epicsMutexId lock;
    Snapshot *head;
    Snapshot *tail;
} ThreadExampleContext;

typedef struct WorkerArgs {
    Snapshot *snapshot;
} WorkerArgs;

static int isPrime(long long value)
{
    long long divisor;

    if (value < 2)
        return 0;
    if (value == 2)
        return 1;
    if ((value % 2) == 0)
        return 0;

    for (divisor = 3; divisor * divisor <= value; divisor += 2) {
        if ((value % divisor) == 0)
            return 0;
    }
    return 1;
}

static Snapshot *snapshotPop(ThreadExampleContext *context)
{
    Snapshot *snapshot;

    /*
     Dispatcher side of the shared queue.
     The mutex prevents races while removing the oldest pending Snapshot.
     */
    epicsMutexLock(context->lock);
    snapshot = context->head;
    if (snapshot) {
        context->head = snapshot->next;
        if (!context->head)
            context->tail = NULL;
    }
    epicsMutexUnlock(context->lock);

    return snapshot;
}

static void snapshotPush(ThreadExampleContext *context, Snapshot *snapshot)
{
    snapshot->next = NULL;

    /*
     Callback side of the shared queue.
     The mutex protects head/tail while inserting a new Snapshot.
     */
    epicsMutexLock(context->lock);
    if (context->tail)
        context->tail->next = snapshot;
    else
        context->head = snapshot;
    context->tail = snapshot;
    epicsMutexUnlock(context->lock);
    /*
     Wake the dispatcher after the Snapshot has been fully inserted.
     The callback does not wait for the worker calculation.
     */
    epicsEventSignal(context->wakeup);
}
    /*
     The worker owns a private heap Snapshot.
     It does not access the EPICS record or the shared queue, so the
     prime calculation cannot block PV processing.
     */
static void workerThread(void *userArg)
{
    WorkerArgs *args = userArg;
    Snapshot *snapshot = args->snapshot;
    double total = 0.0;
    long long sum;
    char stampText[64];

    for (long i = 0; i < snapshot->count; ++i)
        total += snapshot->values[i];

    sum = llround(total);

    printf("New worker threadId : %lu\n", (unsigned long)epicsThreadGetIdSelf());
    printf(" sum = %lld\n", sum);

    //DEBUG
    printf(" count = %ld values:", snapshot->count);
    for (long i = 0; i < snapshot->count && i < 10; ++i) {
        printf(" %.0f", snapshot->values[i]);
    }
    printf("\n");

    if (fabs(total - (double)sum) < 1e-6 && isPrime(sum)) {
        epicsTimeToStrftime(stampText, sizeof(stampText), "%Y-%m-%dT%H:%M:%S.%09f", &snapshot->stamp);
        printf("worker : data is prim : %lld at %s Z\n", sum, stampText);
    }

    fflush(stdout);
    free(snapshot);
    free(args);
}
/*
         * Sleep until the callback signals that at least one Snapshot is queued.
         * This avoids busy-waiting and keeps the dispatcher inactive when idle.
         */
static void dispatcherThread(void *userArg)
{
    ThreadExampleContext *context = userArg;

    for (;;) {
        Snapshot *snapshot;

        epicsEventWait(context->wakeup);

        while ((snapshot = snapshotPop(context)) != NULL) {
            /*
             * Each worker receives its own argument object and Snapshot.
             * No shared buffer is used by worker threads.
             */
            WorkerArgs *args = malloc(sizeof(*args));
            char threadName[64];

            if (!args) {
                free(snapshot);
                continue;
            }

            args->snapshot = snapshot;
            snprintf(threadName, sizeof(threadName), "threadExampleW%p", (void *)snapshot);
            /*
             * Start the expensive calculation in a low-priority worker thread.
             * The dispatcher immediately continues with the next queued Snapshot.
             */
            if (!epicsThreadCreate(threadName,
                                   epicsThreadPriorityLow,
                                   epicsThreadGetStackSize(epicsThreadStackMedium),
                                   workerThread,
                                   args)) {
                free(snapshot);
                free(args);
            }
        }
    }
}

static void eventCallback(void *userArg,
                          struct dbChannel *chan,
                          int eventsRemaining,
                          struct db_field_log *pfl)
{
    ThreadExampleContext *context = userArg;
    Snapshot *snapshot;
    long count;
    long sourceCount;
    long offset = 0;
    void *source = NULL;
    int locked = 0;

    (void)eventsRemaining;

    count = dbChannelFinalElements(chan);
    if (count <= 0)
        return;
    /*
     * if the event does not already provide a stable field-log copy,
     * lock the record while reading the array pointer and metadata.
     * the lock is held only for the short copy operation.
     */
    if (!dbfl_has_copy(pfl)) {
        dbScanLock(dbChannelRecord(chan));
        locked = 1;
    }
    /*
     get the current array pointer, number of valid elements and ring-buffer
     offset from the compress record's VAL field.
     */
    sourceCount = count;
    dbChannelGetArrayInfo(chan, &source, &sourceCount, &offset);
    if (!source || sourceCount <= 0)
        goto done;

    snapshot = malloc(sizeof(*snapshot) + (size_t)(sourceCount - 1u) * sizeof(double));
    if (!snapshot)
        goto done;
    
     // Copy the circular buffer into a linear Snapshot.
     // After this point the worker can process the values independently.
     
    dbExtractArray(source,
                   snapshot->values,
                   sizeof(double),
                   sourceCount,
                   sourceCount,
                   offset,
                   1);

    snapshot->count = sourceCount;
    snapshot->stamp = dbChannelRecord(chan)->time;

    /* DEBUG NOT NECESSARY ANYMORE
    printf("callback copied count=%ld values:", snapshot->count);
    for (long i = 0; i < snapshot->count && i < 10; ++i) {
        printf(" %.0f", snapshot->values[i]);
    }
    printf("\n");
    */

    
     //Queue the Snapshot and signal the dispatcher.
     //No prime calculation is done in the callback.
     
    snapshotPush(context, snapshot);

done:
    if (locked)
        dbScanUnlock(dbChannelRecord(chan));
}
static int endsWithVal(const char *name)
{
    size_t length = strlen(name);

    return length >= 4u && strcmp(name + length - 4u, ".VAL") == 0;
}

static char *makeChannelName(const char *name, const char *suffix)
{
    size_t nameLen = strlen(name);
    size_t suffixLen = strlen(suffix);
    char *result = malloc(nameLen + suffixLen + 1u);

    if (!result)
        return NULL;

    memcpy(result, name, nameLen);
    memcpy(result + nameLen, suffix, suffixLen + 1u);
    return result;
}

static void threadExample(const char *pvName)
{
    ThreadExampleContext *context;
    char *valueName;
    long status;
    long arraySize;

    if (!pvName || !*pvName) {
        fprintf(stderr, "threadExample: PV-Name fehlt\n");
        return;
    }

    valueName = endsWithVal(pvName) ? epicsStrDup(pvName) : makeChannelName(pvName, ".VAL");
    if (!valueName) {
        fprintf(stderr, "threadExample: kein Speicher\n");
        free(valueName);
        return;
    }

    context = calloc(1u, sizeof(*context));
    if (!context) {
        fprintf(stderr, "threadExample: kein Speicher\n");
        free(valueName);
        return;
    }

     
     //Create synchronization primitives before enabling the subscription.
     //The mutex protects the queue; the event wakes the dispatcher.
     
    context->lock = epicsMutexCreate();
    context->wakeup = epicsEventCreate(epicsEventEmpty);
    context->eventCtx = db_init_events();
    if (!context->lock || !context->wakeup || !context->eventCtx) {
        fprintf(stderr, "threadExample: Initialisierung fehlgeschlagen\n");
        free(valueName);
        return;
    }

    status = db_start_events(context->eventCtx, "threadExampleEvt", NULL, NULL, epicsThreadPriorityLow);
    if (status != DB_EVENT_OK) {
        fprintf(stderr, "threadExample: db_start_events fehlgeschlagen\n");
        free(valueName);
        return;
    }

    context->valueChan = dbChannelCreate(valueName);
    if (!context->valueChan || dbChannelOpen(context->valueChan)) {
        fprintf(stderr, "threadExample: '%s' konnte nicht geoeffnet werden\n", valueName);
        if (context->valueChan)
            dbChannelDelete(context->valueChan);
        free(valueName);
        return;
    }

    arraySize = dbChannelFinalElements(context->valueChan);
    if (arraySize <= 0) {
        fprintf(stderr, "threadExample: '%s' hat keine Array-Daten\n", pvName);
        dbChannelDelete(context->valueChan);
        free(valueName);
        return;
    }

    if (!endsWithVal(pvName)) {
        char *fieldName = makeChannelName(pvName, ".VAL");
        if (fieldName) {
            free(valueName);
            valueName = fieldName;
        }
    }
    /*
     * Subscribe to value changes of the compress record.
     * The callback only copies data and signals the dispatcher.
     */
    context->subscription = db_add_event(context->eventCtx, context->valueChan, eventCallback, context, DBE_VALUE);
    if (!context->subscription) {
        fprintf(stderr, "threadExample: Subscription fehlgeschlagen\n");
        dbChannelDelete(context->valueChan);
        free(valueName);
        return;
    }

    db_event_enable(context->subscription);

    context->head = NULL;
    context->tail = NULL;
     
     //The dispatcher is a long-running medium-priority thread.
     //It waits for queued Snapshots and starts low-priority workers.
     
    if (!epicsThreadCreate("threadExampleDisp",
                           epicsThreadPriorityMedium,
                           epicsThreadGetStackSize(epicsThreadStackMedium),
                           dispatcherThread,
                           context)) {
        fprintf(stderr, "threadExample: Dispatcher konnte nicht gestartet werden\n");
        dbChannelDelete(context->valueChan);
        free(valueName);
        return;
    }

    printf("threadExample: '%s' VAL elements=%ld\n", pvName, arraySize);
    printf("threadExample: subscription active for '%s'\n", pvName);

    free(valueName);
}

static const iocshArg threadExampleArg0 = {"pv-name", iocshArgString};
static const iocshArg * const threadExampleArgs[] = {&threadExampleArg0};
static const iocshFuncDef threadExampleFuncDef = {
    "threadExample",
    1,
    threadExampleArgs,
#ifdef IOCSHFUNCDEF_HAS_USAGE
    "Check on a given compress record that the sum of val-array is prime\n"
    "Output of the timestamp if prim. The calculation must not influence the PV process.\n"
    "Example: threadExample $(user):compressExample\n",
#endif
};

static void threadExampleCallFunc(const iocshArgBuf *args)
{
    threadExample(args[0].sval);
}

static void threadExampleRegister(void)
{
    iocshRegister(&threadExampleFuncDef, threadExampleCallFunc);
}

epicsExportRegistrar(threadExampleRegister);