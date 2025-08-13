#include <stdio.h>
#include <sys/types.h>
#include <sys/shm.h>
#include <sys/ipc.h>
#include <mqueue.h>
#include <sys/wait.h>
#include <sys/msg.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <ctype.h>
#include <limits.h>
#include <pthread.h>

#pragma GCC optimize("O2,fast-math")
#pragma GCC target("sse4.2")

#define MAX_NEW_REQUESTS 100
#define MAX_DOCKS 30
#define MAX_AUTH_STRING_LEN 100
#define MAX_CARGO_COUNT 200
#define MAX_GUESS_STRING_LEN 11

int mqid = -1;

typedef struct MessageStruct
{
    long mtype;
    int timestep;
    int shipId;
    int direction;
    int dockId;
    int cargoId;
    int isFinished;
    union
    {
        int numShipRequests;
        int craneId;
    };
} MessageStruct;

typedef struct ShipRequest
{
    int shipId;
    int timestep;
    int category;
    int direction;
    int emergency;
    int waitingTime;
    int numCargo;
    int cargo[MAX_CARGO_COUNT];
} ShipRequest;

typedef struct MainSharedMemory
{
    char authStrings[MAX_DOCKS][MAX_AUTH_STRING_LEN];
    ShipRequest newShipRequests[MAX_NEW_REQUESTS];
} MainSharedMemory;

typedef struct SolverRequest
{
    long mtype;
    int dockId;
    char authStringGuess[MAX_AUTH_STRING_LEN];
} SolverRequest;

typedef struct SolverResponse
{
    long mtype;
    int guessIsCorrect;
} SolverResponse;

typedef struct
{
    int solver_id;
    int dockId;
    int len;
    int l;
    int r;
} SolverArgs;

typedef struct Dock //Contains dock information including docked ShipRequest
{
    int dockId;
    int category;
    int cranes[25];
    int dockEmpty;
    ShipRequest *s;
} Dock;

typedef struct ISRMatrix
{
    int matrix[26][1000];
    int pushpointers[26];
    int poppointers[26];
} ISRMatrix;

typedef struct OSRMatrix
{
    int matrix[26][1000];
    int pushpointers[26];
    int poppointers[26];
} OSRMatrix;

typedef struct ESRMatrix
{
    int matrix[26][150];
    int pushpointers[26];
    int poppointers[26];
} ESRMatrix;

int *getInputs(int testcase)
{
    char fname[100];
    snprintf(fname, sizeof(fname), "./testcase%d/input.txt", testcase);
    FILE *file = fopen(fname, "r");

    if (file == NULL)
    {
        fprintf(stderr, "Error opening %s: %s\n", fname, strerror(errno));
        exit(EXIT_FAILURE);
    }

    char line[200];

    int *arr = (int *)malloc(1000 * sizeof(int));

    int i = 0;

    while (fgets(line, sizeof(line), file))
    {
        char *token = strtok(line, " \t\n");
        while (token != NULL)
        {
            if (sscanf(token, "%d", &arr[i]) == 1)
                i++;
            else
                fprintf(stderr, "Invalid integer at line: %s\n", token);

            token = strtok(NULL, " \t\n");
        }
    }

    fclose(file);
    return arr;
}

int createMessageQueue(key_t key)
{
    int mqid = msgget(key, 0666);
    if (mqid == -1)
    {
        fprintf(stderr, "Failed to join message queue with key %d\n", key);
        exit(EXIT_FAILURE);
    }
    return mqid;
}

MessageStruct receiveShipRequests()
{
    MessageStruct request;
    memset(&request, 0, sizeof(request));
    if (msgrcv(mqid, &request, sizeof(MessageStruct) - sizeof(long), (long)1, 0) == -1)
    {
        fprintf(stderr, "Failed to receive ship request message (mtype 1) from validation\n");
        exit(EXIT_FAILURE);
    }

    // Handles termination
    if (request.isFinished == 1)
        exit(EXIT_SUCCESS);

    return request;
}

void sendNextTimestepMessage()
{ // WORKING
    MessageStruct updateRequest;
    updateRequest.mtype = 5;
    if (msgsnd(mqid, &updateRequest, sizeof(MessageStruct) - sizeof(long), 0) == -1)
    {
        fprintf(stderr, "Failed to send timestep update request to validation");
        exit(EXIT_FAILURE);
    }
}

void sendDockingMessage(int dockId, ShipRequest *s)
{ // WORKING
    MessageStruct dockingMessage;
    dockingMessage.mtype = 2, dockingMessage.dockId = dockId, dockingMessage.shipId = s->shipId, dockingMessage.direction = s->direction;
    if (msgsnd(mqid, &dockingMessage, sizeof(MessageStruct) - sizeof(long), 0) == -1)
    {
        fprintf(stderr, "Failed to send docking message to validation");
        exit(EXIT_FAILURE);
    }
}

void sendCargoMessage(Dock *d, int cargoid, int craneid)
{ // WORKING
    MessageStruct cargoMessage;
    cargoMessage.mtype = 4, cargoMessage.dockId = d->dockId, cargoMessage.shipId = d->s->shipId, cargoMessage.direction = d->s->direction;
    cargoMessage.cargoId = cargoid, cargoMessage.craneId = craneid;
    if (msgsnd(mqid, &cargoMessage, sizeof(MessageStruct) - sizeof(long), 0) == -1)
    {
        fprintf(stderr, "Failed to send cargo message to validation");
        exit(EXIT_FAILURE);
    }
}

void sendUndockingMessage(Dock *d)
{
    MessageStruct undockingMessage;
    undockingMessage.mtype = 3, undockingMessage.dockId = d->dockId;
    undockingMessage.shipId = d->s->shipId, undockingMessage.direction = d->s->direction;
    if (msgsnd(mqid, &undockingMessage, sizeof(MessageStruct) - sizeof(long), 0) == -1)
    {
        fprintf(stderr, "Failed to send undocking message to validation");
        exit(EXIT_FAILURE);
    }
    d->dockEmpty = 1;
}

void sendDockInfo(int dockId, int solverMqid)
{
    SolverRequest request;
    request.mtype = 1, request.dockId = dockId;
    if (msgsnd(solverMqid, &request, sizeof(SolverRequest) - sizeof(long), 0) == -1)
    {
        fprintf(stderr, "Failed to send dock info to solver");
        exit(EXIT_FAILURE);
    }
}

int *inputs;

key_t shm_key, mq_key;

int solver_count, dock_count;

int solver_mqids[8];

int cur_timestep, request_count;

MainSharedMemory *shm_ptr;

Dock docks[30];
int dockAction[30], movedCargo[30], dockedTime[30];
ShipRequest incomingShipRequestQueue[1000], outgoingShipRequestQueue[1000], emergencyShipRequestQueue[150];
int incomingPush = 1, outgoingPush = 1, emergencyPush = 1;

ISRMatrix isrmat;
OSRMatrix osrmat;
ESRMatrix esrmat;

int undockQueue[1100][2];
int undockQueuePushIndex = 0, undockQueuePopIndex = 0;

char ***auths = NULL;
int *authSizes = NULL;

int guessingRanges[MAX_GUESS_STRING_LEN][8][2];

int found = 0;

int power6(int n)
{
    int res = 1;
    while (n-- > 0)
        res *= 6;
    return res;
}

int getAuthCount(int index)
{
    if (index == 0)
        return 1;
    if (index == 1)
        return 5;
    if (index == 2)
        return 25;
    return 25 * power6(index - 2);
}

void initAuth()
{
    auths = malloc((MAX_GUESS_STRING_LEN) * sizeof(char **));
    authSizes = malloc((MAX_GUESS_STRING_LEN) * sizeof(int));
    for (int i = 0; i < MAX_GUESS_STRING_LEN; i++)
    {
        int count = getAuthCount(i);
        authSizes[i] = count;
        auths[i] = malloc(count * sizeof(char *));
        for (int j = 0; j < count; j++)
        {
            auths[i][j] = calloc(MAX_GUESS_STRING_LEN, sizeof(char));
        }
    }
}


void receiveAndPushShipRequests()
{
    MessageStruct request = receiveShipRequests();

    request_count = request.numShipRequests, cur_timestep = request.timestep;
    // Push ship requests into queue
    for (int i = 0; i < request_count; i++)
    {
        int cat = shm_ptr->newShipRequests[i].category;
        ShipRequest *obj = &shm_ptr->newShipRequests[i];
        if (obj->direction == -1 && obj->emergency == 0)
        {
            outgoingShipRequestQueue[outgoingPush] = shm_ptr->newShipRequests[i];
            osrmat.matrix[cat][osrmat.pushpointers[cat]] = outgoingPush;
            osrmat.pushpointers[cat]++;
            outgoingPush++;
        }
        else if (obj->direction == 1 && obj->emergency == 0)
        {
            incomingShipRequestQueue[incomingPush] = shm_ptr->newShipRequests[i];
            isrmat.matrix[cat][isrmat.pushpointers[cat]] = incomingPush;
            isrmat.pushpointers[cat]++;
            incomingPush++;
        }
        else
        {
            emergencyShipRequestQueue[emergencyPush] = shm_ptr->newShipRequests[i];
            esrmat.matrix[cat][esrmat.pushpointers[cat]] = emergencyPush;
            esrmat.pushpointers[cat]++;
            emergencyPush++;
        }
    }
}

void updatePopPointer(int i)
{
    int ind = isrmat.matrix[i][isrmat.poppointers[i]];
    int max_ind = incomingPush;
    ShipRequest *s = &incomingShipRequestQueue[ind];
    while (ind > 0 && ind < max_ind && ((s->timestep + s->waitingTime) < cur_timestep))
    {
        isrmat.poppointers[i]++;
        ind = isrmat.matrix[i][isrmat.poppointers[i]];
        s = &incomingShipRequestQueue[ind];
    }
}

void dock_ships(int i)
{
    if (docks[i].dockEmpty == 1)
    {
        for (int j = docks[i].category; j >= 0; j--)
        {
            int k = esrmat.poppointers[j];
            if (esrmat.matrix[j][k] > 0)
            {
                int index = esrmat.matrix[j][k];
                docks[i].s = &emergencyShipRequestQueue[index];
                docks[i].dockEmpty = 0;
                dockAction[i] = 1, dockedTime[i] = cur_timestep;
                esrmat.poppointers[j]++;
                sendDockingMessage(docks[i].dockId, docks[i].s);
                return;
            }
        }
    }

    if (docks[i].dockEmpty == 1)
    {
        for (int j = docks[i].category; j >= 0; j--)
        {
            int k = isrmat.poppointers[j];
            if (k < isrmat.pushpointers[j] && isrmat.matrix[j][k] > 0)
            {
                int index = isrmat.matrix[j][k];
                docks[i].s = &incomingShipRequestQueue[index];
                docks[i].dockEmpty = 0;
                dockAction[i] = 1, dockedTime[i] = cur_timestep;
                isrmat.poppointers[j]++;
                updatePopPointer(j);
                sendDockingMessage(docks[i].dockId, docks[i].s);
                return;
            }
        }
    }

    if (docks[i].dockEmpty == 1)
    {
        for (int j = docks[i].category; j >= 0; j--)
        {
            int k = osrmat.poppointers[j];
            if (k < osrmat.pushpointers[j] && osrmat.matrix[j][k] > 0)
            {
                int index = osrmat.matrix[j][k];
                docks[i].s = &outgoingShipRequestQueue[index];
                docks[i].dockEmpty = 0;
                dockAction[i] = 1, dockedTime[i] = cur_timestep;
                osrmat.poppointers[j]++;
                sendDockingMessage(docks[i].dockId, docks[i].s);
                return;
            }
        }
    }
}

void handle_cargo(int id) // WORKING
{
    Dock *dock = &docks[id];

    int *cur_crane = dock->cranes;

    for (int i = 0; i < dock->category; i++)
    {
        int *cur_cargo = dock->s->cargo;
        int capacity = *cur_crane;
        int max_wt = -1, cargoId = -1;

        for (int j = 0; j < dock->s->numCargo; j++)
        {
            if (*cur_cargo <= capacity && *cur_cargo > max_wt)
                max_wt = *cur_cargo, cargoId = j;

            cur_cargo++;
        }

        if (cargoId != -1)
        {
            movedCargo[id]++;
            dock->s->cargo[cargoId] = -2;
            sendCargoMessage(dock, cargoId, i);
        }

        if (movedCargo[id] == dock->s->numCargo)
        {
            dockAction[id] = 2;
            movedCargo[id] = 0;
            break;
        }

        cur_crane++;
    }
}

void pushToUndockQueue(int i)
{
    Dock *dock = &docks[i];
    undockQueue[undockQueuePushIndex][0] = dock->dockId;
    undockQueue[undockQueuePushIndex][1] = cur_timestep - dockedTime[i] - 1;
    undockQueuePushIndex++;
}

void generateGuessingRanges(int solver_count)
{
    for (int i = 0; i < solver_count; i++)
    {
        guessingRanges[1][i][0] = 5;
        guessingRanges[1][i][1] = 4;
    }
    guessingRanges[1][solver_count - 1][0] = 0;

    for (int i = 2; i <= MAX_GUESS_STRING_LEN - 1; i++)
    {
        int last = 25;
        for (int j = 2; j < i; j++)
            last *= 6;

        int range = last / solver_count;

        for (int j = 0; j < solver_count - 1; j++)
        {
            guessingRanges[i][j][0] = j * range;
            guessingRanges[i][j][1] = (j + 1) * range - 1;
        }
        guessingRanges[i][solver_count - 1][0] = (solver_count - 1) * range;
        guessingRanges[i][solver_count - 1][1] = last - 1;
    }
}

const char charSet[] = {'5', '6', '7', '8', '9', '.'};

void generator(char **a, int len)
{
    char frequency[len + 1];
    int indices[len];
    memset(indices, 0, sizeof(indices));

    int count = 0;

    while (1)
    {
        for (int i = 0; i < len; i++)
            frequency[i] = charSet[indices[i]];
        frequency[len] = '\0';

        memcpy(a[count], frequency, len);
        a[count][len] = '\0';
        count++;

        int pos = len - 1;
        while (pos >= 0)
        {
            int maxIndex = (pos == 0 || pos == len - 1) ? 4 : 5;
            indices[pos]++;
            if (indices[pos] <= maxIndex)
                break;
            else
            {
                indices[pos] = 0;
                pos--;
            }
        }

        if (pos < 0)
            break;
    }
}

void *solver(void *arg)
{
    SolverArgs *args = (SolverArgs *)arg;
    int solver_id = args->solver_id;
    int dockId = args->dockId;
    int len = args->len;
    int l = args->l;
    int r = args->r;

    free(arg);

    if (len >= MAX_GUESS_STRING_LEN)
    {
        fprintf(stderr, "WARNING: Length of auth string guess is %d characters for dock ID %d and ship ID: %d, %d.\n",
                len, dockId, docks[dockId].s->shipId, docks[dockId].s->direction);
    }

    int solverMqid = solver_mqids[solver_id];

    SolverRequest guess;
    memset(&guess, 0, sizeof(guess));
    guess.mtype = 2;
    SolverResponse response;

    for (int i = l; i <= r; i++)
    {
        if (__sync_fetch_and_add(&found, 0))
            return NULL;

        memcpy(guess.authStringGuess, auths[len][i], len + 1);

        if (msgsnd(solverMqid, &guess, sizeof(SolverRequest) - sizeof(long), 0) == -1)
        {
            fprintf(stderr, "Failed to send guess to solver");
            exit(EXIT_FAILURE);
        }

        memset(&response, 0, sizeof(response));

        if (msgrcv(solverMqid, &response, sizeof(SolverResponse) - sizeof(long), (long)3, 0) == -1)
        {
            fprintf(stderr, "Failed to receive response from solver");
            exit(EXIT_FAILURE);
        }

        if (response.guessIsCorrect == 1)
        {
            __sync_lock_test_and_set(&found, 1);
            memset(shm_ptr->authStrings[dockId], 0, sizeof(shm_ptr->authStrings[dockId]));
            strncpy(shm_ptr->authStrings[dockId], auths[len][i], MAX_AUTH_STRING_LEN);
            __sync_synchronize();
            sendUndockingMessage(&docks[dockId]);
            printf("Dock %d guessed correct auth: %s\n", dockId, auths[len][i]);
            dockAction[dockId] = 0;
            break;
        }
    }
    return NULL;
}

int main(int argc, char *argv[])
{   
    if (argc != 2)
    {
        fprintf(stderr, "Error: Missing test case number.\n");
        exit(EXIT_FAILURE);
    }

    int testcase = atoi(argv[1]);
    inputs = getInputs(testcase);

    shm_key = inputs[0], mq_key = inputs[1];

    solver_count = inputs[2], dock_count = inputs[3 + solver_count];

    // Join validation message queue
    mqid = createMessageQueue(mq_key);

    // Join solvers' message queues
    for (int i = 0; i < solver_count; i++)
    {
        key_t solver_key = inputs[3 + i];
        solver_mqids[i] = createMessageQueue(solver_key);
    }

    // Join shared memory
    int shmid = shmget(shm_key, 0, 0666);
    if (shmid == -1)
    {
        fprintf(stderr, "shmget failed (key = %d, size = %lu): %s\n", shm_key, sizeof(MainSharedMemory), strerror(errno));
        exit(EXIT_FAILURE);
    }

    shm_ptr = (MainSharedMemory *)shmat(shmid, NULL, 0);
    if (shm_ptr == (MainSharedMemory *)-1)
    {
        perror("shmat failed");
        exit(EXIT_FAILURE);
    }

    // Store dock and crane details
    for (int i = 0, cur_index = 4 + solver_count; i < dock_count; i++)
    {
        docks[i].dockId = i, docks[i].dockEmpty = 1;
        int sz = inputs[cur_index];
        docks[i].category = sz;
        cur_index++;
        for (int j = 0; j < sz; j++)
        {
            docks[i].cranes[j] = inputs[cur_index];
            cur_index++;
        }
    }

    memset(incomingShipRequestQueue, 0, sizeof(incomingShipRequestQueue));
    memset(outgoingShipRequestQueue, 0, sizeof(outgoingShipRequestQueue));
    memset(emergencyShipRequestQueue, 0, sizeof(emergencyShipRequestQueue));
    memset(isrmat.pushpointers, 0, sizeof(isrmat.pushpointers));
    memset(esrmat.pushpointers, 0, sizeof(esrmat.pushpointers));
    memset(osrmat.pushpointers, 0, sizeof(osrmat.pushpointers));
    memset(isrmat.poppointers, 0, sizeof(isrmat.poppointers));
    memset(esrmat.poppointers, 0, sizeof(esrmat.poppointers));
    memset(osrmat.poppointers, 0, sizeof(osrmat.poppointers));

    for (int i = 0; i < 26; i++)
        for (int j = 0; j < 150; j++)
            esrmat.matrix[i][j] = 0;

    for (int i = 0; i < 26; i++)
        for (int j = 0; j < 1000; j++)
        {
            isrmat.matrix[i][j] = 0;
            osrmat.matrix[i][j] = 0;
        }

    for (int i = 0; i < dock_count; i++)
    {
        dockAction[i] = 0;
        movedCargo[i] = 0;
        dockedTime[i] = 0;
    }

    memset(undockQueue, 0, sizeof(undockQueue));

    initAuth();
    for (int i = 1; i <= MAX_GUESS_STRING_LEN - 1; i++)
        generator(auths[i], i);

    generateGuessingRanges(solver_count);

    while (1)
    {
        // Receive and push ship requests into queue and matrix
        receiveAndPushShipRequests();

        // Send back ships which have exceeded their waiting time
        for (int i = 0; i < 26; i++)
            updatePopPointer(i);

        // Dock/handle cargo/undock based on dockAction flag
        for (int i = 0; i < dock_count; i++)
        {
            if (dockAction[i] == 0)
                dock_ships(i);
            else if (dockAction[i] == 1)
                handle_cargo(i);
            else if (dockAction[i] == 2)
                pushToUndockQueue(i);
        }
        printf("Current timestep: %d\n", cur_timestep);
        while (undockQueuePopIndex < undockQueuePushIndex)
        {
            found = 0;

            int dockId = undockQueue[undockQueuePopIndex][0];
            int len = undockQueue[undockQueuePopIndex][1];
            undockQueuePopIndex++;
            printf("Undocking ship at dock %d with password length %d\n", dockId, len);

            for (int i = 0; i < solver_count; i++)
                sendDockInfo(dockId, solver_mqids[i]);

            pthread_t threads[solver_count];
            for (int i = 0; i < solver_count; i++)
            {
                int l = guessingRanges[len][i][0], r = guessingRanges[len][i][1];

                SolverArgs *args = (SolverArgs *)malloc(sizeof(SolverArgs));
                args->solver_id = i;
                args->dockId = dockId;
                args->len = len;
                args->l = l;
                args->r = r;
                pthread_create(&threads[i], NULL, solver, (void *)args);
            }
            for (int i = 0; i < solver_count; i++)
            {
                pthread_join(threads[i], NULL);
            }
        }
        sendNextTimestepMessage();
    }
}