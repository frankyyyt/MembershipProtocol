/**********************
*
* Progam Name: MP1. Membership Protocol
* 
* Code authors: Udit Mehrotra, NetID : umehrot2
*
* Current file: mp1_node.c
* About this file: Member Node Implementation
* 
***********************/

#include "mp1_node.h"
#include "emulnet.h"
#include "MPtemplate.h"
#include "log.h"
#define TFAIL 10
#define TCLEANUP 10

/*
 *
 * Routines for introducer and current time.
 *
 */

char NULLADDR[] = {0,0,0,0,0,0};
int isnulladdr( address *addr){
    return (memcmp(addr, NULLADDR, 6)==0?1:0);
}

/* 
Return the address of the introducer member. 
*/
address getjoinaddr(void){

    address joinaddr;

    memset(&joinaddr, 0, sizeof(address));
    *(int *)(&joinaddr.addr)=1;
    *(short *)(&joinaddr.addr[4])=0;

    return joinaddr;
}

/*
 *
 * Message Processing routines.
 *
 */

/*
 Adds a Node to membership table of the Introducer
*/
void addNode(void *env,char *data)
{
    member *node = (member *) env;
    
    //create a new membership Entry object
    struct memEntry t;
    t.addr.addr[0] = data[0];
    t.addr.addr[1] = data[1];
    t.addr.addr[2] = data[2];
    t.addr.addr[3] = data[3];
    t.addr.addr[4] = data[4];
    t.addr.addr[5] = data[5];
    t.heartbeat = 0;
    t.flag = 0;
    t.time = globaltime;
    
    //Check to find the first deleted entry in the table where new entry can be added
    int i=0;
    while(i < node->memCount)
    {
        if(node->ml[i].flag == 1)
            break;
        i++;
    }
    
    //If a deleted entry is found, add the new entry there, else append it to the table at the end
    if(i < node->memCount)
    {
        node->ml[i] = t;
    }
    else
    {
        node->ml[node->memCount] = t;
        node->memCount++;
    }
    
    //Log the node added
    #ifdef DEBUGLOG
        logNodeAdd(&node->addr,&t.addr);
    #endif
    

}

/* 
Received a JOINREQ (joinrequest) message.
*/
void Process_joinreq(void *env, char *data, int size)
{

    member *node = (member *) env;
    
    #ifdef DEBUGLOG
        static char s[1024];
    #endif
    
    #ifdef DEBUGLOG
        LOG(&((member *)env)->addr, "Processing Join Request !!");
    #endif
    
    /* create JOINREP message: format of data is {struct address myaddr} */
    
    #ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
    LOG(&node->addr, s);
    #endif
    
    //Add the node to introducers membership table
    addNode(env,data);

    //Create a new JOIN REPLY message
    size_t msgsize = sizeof(messagehdr) + sizeof(memEntry *);
    messagehdr *msg = malloc(msgsize);
    msg->msgtype = JOINREP;

    //Fetch the Membership list that is to be sent in JOIN REPLY
    struct memEntry *mlist = malloc(MAX_NNB * sizeof(memEntry));
    int loop = 0;
    struct memEntry *a = node->ml;
    while(a->addr.addr[0] != '\0')
    {
        mlist->addr =  a->addr;
        mlist->heartbeat = a->heartbeat;
        mlist->flag = a->flag;
        mlist->time = a->time;
        a++;
        mlist++;
        loop++;
    }
    mlist = mlist - loop;
    
    //Add the membership list in message + 1
    memcpy((char *) (msg +1),&mlist, sizeof(memEntry *));
    
    /* send JOINREP message to introducer member. */
    MPp2psend(&node->addr, data, (char *)msg, msgsize);

    free(msg);
    
    return;
}




/*
This function merges the membership list passed, with the membership list of the node
 */
void load_membershipTable(void *env, void *mlist)
{
    
    int count;
    member *node = (member *) env;
    memEntry *list = (memEntry *)mlist;
    
    if(list->addr.addr != NULL)
    {
       
        //Loop through all the entries in the membership list to be merged
        while(list->addr.addr[0] != '\0')
        {
        
            if(list->flag == 0)
            {
                count = 0;
                int j=0;
                
                //Loop through all the entries in the node's membership list
                while(node->ml[j].addr.addr[0] != '\0')
                {
                    if(memcmp(&node->ml[j].addr,&list->addr,4*sizeof(char)) == 0)
                    {
                        //Check if entry already there is nodes membership table and set flag count
                        count = 1;
                        break;
                    }
            
                    j++;
                }
        
                if(count == 0)
                {
                    //This means that the entry is not there in node's membership table
                    //Find the first deleted entry where the new entry can be added, else append it
                    int x=0;
                    while(x < node->memCount)
                    {
                        if(node->ml[x].flag == 1)
                            break;
                        x++;
                    }
            
                    node->ml[x].addr = list->addr;
                    node->ml[x].heartbeat = list->heartbeat;
                    node->ml[x].flag = list->flag;
                    node->ml[x].time = list->time;
            
                    //Log the newly added node to the membership table
                    #ifdef DEBUGLOG
                        logNodeAdd(&node->addr, &list->addr);
                    #endif
                    
                    if(x == node->memCount)
                        node->memCount++;
                }
                else
                {
                    //check for better heartbeats, for non deleted nodes in the membership table
                    if(node->ml[j].flag != 1)
                    {
                        if(node->ml[j].heartbeat < list->heartbeat)
                        {
                            node->ml[j].heartbeat = list->heartbeat;
                            node->ml[j].time = globaltime;
                            node->ml[j].flag = 0;
                        }
                    }
            
                }
            }
        

            list = list + 1;
        }
    }

}

/*
Received a JOINREP (joinreply) message. 
*/
void Process_joinrep(void *env, char *data, int size)
{

    member *node = (member *) env;
    memEntry **temp = (memEntry **) data;
    memEntry *node1 = *temp;
    
    //Merge the current node's membership list with introducers list
    load_membershipTable(node, node1);
    
    //Set current node's ingroup to 1
    node->ingroup = 1;
    
    #ifdef DEBUGLOG
        LOG(&((member *)env)->addr, "Processing Rep Request !!");
    #endif
     
    
    return;
}


/*
 Process a received a GOSSIP (gossip) message.
 */
void Process_gossip(void *env, char *data, int size)
{
    member *node = (member *) env;
    memEntry **temp = (memEntry **) data;
    memEntry *node1 = *temp;
    
    //Merge membership table from the gossip message with current nodes membership table
    load_membershipTable(node, node1);
}

/*
Array of Message handlers. 
*/
void ( ( * MsgHandler [20] ) STDCLLBKARGS )={
/* Message processing operations at the P2P layer. */
    Process_joinreq, 
    Process_joinrep,
    Process_gossip
};

/* 
Called from nodeloop() on each received packet dequeue()-ed from node->inmsgq. 
Parse the packet, extract information and process. 
env is member *node, data is 'messagehdr'. 
*/
int recv_callback(void *env, char *data, int size){

    member *node = (member *) env;
    messagehdr *msghdr = (messagehdr *)data;
    char *pktdata = (char *)(msghdr+1);

    if(size < sizeof(messagehdr)){
#ifdef DEBUGLOG
        LOG(&((member *)env)->addr, "Faulty packet received - ignoring");
#endif
        return -1;
    }

#ifdef DEBUGLOG
    LOG(&((member *)env)->addr, "Received msg type %d with %d B payload", msghdr->msgtype, size - sizeof(messagehdr));
#endif

    if((node->ingroup && msghdr->msgtype >= 0 && msghdr->msgtype <= DUMMYLASTMSGTYPE)
        || (!node->ingroup && msghdr->msgtype==JOINREP))            
            /* if not yet in group, accept only JOINREPs */
        MsgHandler[msghdr->msgtype](env, pktdata, size-sizeof(messagehdr));
    /* else ignore (garbled message) */
    free(data);

    return 0;

}

/*
 *
 * Initialization and cleanup routines.
 *
 */

/* 
Find out who I am, and start up. 
*/
int init_thisnode(member *thisnode, address *joinaddr){
    
    if(MPinit(&thisnode->addr, PORTNUM, (char *)joinaddr)== NULL){ /* Calls ENInit */
#ifdef DEBUGLOG
        LOG(&thisnode->addr, "MPInit failed");
#endif
        exit(1);
    }
#ifdef DEBUGLOG
    else LOG(&thisnode->addr, "MPInit succeeded. Hello.");
#endif

    //Initialize the values for the newly inited node, and allocate its membership table
    thisnode->bfailed=0;
    thisnode->inited=1;
    thisnode->ingroup=0;
    thisnode->ml = malloc(MAX_NNB*sizeof(memEntry));
    thisnode->memCount = 1;
    thisnode->ml[0].addr = thisnode->addr;
    thisnode->ml[0].heartbeat = 0;
    thisnode->ml[0].flag = 0;
    thisnode->ml[0].time = globaltime;

    /* node is up! */
    return 0;
}


/* 
Clean up this node. 
*/
int finishup_thisnode(member *node){

    free(node->ml); //de-allocating the membership table
    memcpy(&node->addr,NULLADDR,6 * sizeof(char)); //Converting node address to null
    LOG(&node->addr,"Cleaned up the node");

    return 0;
}


/* 
 *
 * Main code for a node 
 *
 */

/* 
Introduce self to group. 
*/
int introduceselftogroup(member *node, address *joinaddr){
    
    messagehdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if(memcmp(&node->addr, joinaddr, 4*sizeof(char)) == 0){
        /* I am the group booter (first process to join the group). Boot up the group. */
#ifdef DEBUGLOG
        LOG(&node->addr, "Starting up group...");
#endif

        node->ingroup = 1;
    }
    else{
        size_t msgsize = sizeof(messagehdr) + sizeof(address);
        msg=malloc(msgsize);

    /* create JOINREQ message: format of data is {struct address myaddr} */
        msg->msgtype=JOINREQ;
        memcpy((char *)(msg+1), &node->addr, sizeof(address));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        LOG(&node->addr, s);
#endif

    /* send JOINREQ message to introducer member. */
        MPp2psend(&node->addr, joinaddr, (char *)msg, msgsize);
        
        free(msg);
    }

    return 1;

}

/* 
Called from nodeloop(). 
*/
void checkmsgs(member *node){
    void *data;
    int size;

    /* Dequeue waiting messages from node->inmsgq and process them. */
	
    while((data = dequeue(&node->inmsgq, &size)) != NULL) {
        recv_callback((void *)node, data, size); 
    }
    return;
}

/*
 Called periodically from nodelooops() to check for any Membership entry for which Tfail or Tcleanup time
 has passed, and set them to deleted or suspended state
*/
void checkDelete(void *env)
{
    member *node = (member *) env;
    int i = 1;
    
    //Loop through all the entries in the membership list of the node
    while(node->ml[i].addr.addr[0] != '\0')
    {
        if(node->ml[i].flag != 1)
        {
            if(globaltime > (node->ml[i].time+ TFAIL))
            {
                //Set Suspended flag if TFAIL time has passed
                node->ml[i].flag = 2;
            }
        
            if(globaltime > (node->ml[i].time + TFAIL + TCLEANUP))
            {
                //Set Deleted flag if TFAIL & TCLEANUP time has passed
                node->ml[i].flag = 1;
                
                //Log the deleted node
                #ifdef DEBUGLOG
                    logNodeRemove(&node->addr, &node->ml[i].addr);
                #endif
            }
        }
        i++;
    }
}

/*
Returns a random number in the range 1 to n-1
 */
int random_num(int n)
{
    int random = 0;
    
    while(random == 0)
        random = rand() % n;
    
    return random;
}

/* 
Executed periodically for each member. 
Performs necessary periodic operations. 
Called by nodeloop(). It picks a random other node to gossip its membership list.
*/
void nodeloopops(member *node){


    int random = 0;
    if(node->ingroup == 1)
    {
        int count = node->memCount;
        
        int i = 1;
        int flag = 0;
        
        //Check if there is a non deleted/suspended entry in the membership table to GOSSIP to
        while(i < node->memCount)
        {
            if(node->ml[i].flag == 0)
            {
                //Found atleast one alive entry, so can go ahead with gossip
                flag = 1;
                break;
            }
            i++;
        }
        
        if(flag == 1)
        {
            /* Get a random number for indexing the membership list, such that the number is not 0, since first entry in each members table is its own entry, and the entry at the number is not deleted/suspended */
            while(node->ml[random].flag != 0 || random == 0)
            {
                random = random_num(count);
                int i = 0;
                while(i < node->memCount)
                {
                    i++;
                }
            }
        
       
            /* Create a message of type GOSSIP and send across the membership table */
            size_t msgsize = sizeof(messagehdr) + sizeof(memEntry *);
            messagehdr *msg=malloc(msgsize);
            msg->msgtype=GOSSIP;
        
            node->ml[0].heartbeat++;
            node->ml[0].time = globaltime;
            struct address a = node->ml[random].addr;
            memcpy((char *) (msg +1),&node->ml, sizeof(memEntry *));
    
            MPp2psend(&node->addr, &a, (char *)msg, msgsize);
    
            
            free(msg);
       }
        
        checkDelete(node);

    }
    
    return;
}


/*
Executed periodically at each member. Called from app.c.
*/
void nodeloop(member *node){
    if (node->bfailed) return;

    checkmsgs(node);

    /* Wait until you're in the group... */
    if(!node->ingroup) return ;

    /* ...then jump in and share your responsibilites! */
    nodeloopops(node);
    
    return;
}

/* 
All initialization routines for a member. Called by app.c. 
*/
void nodestart(member *node, char *servaddrstr, short servport){

    address joinaddr=getjoinaddr();
    
    /* Self booting routines */
    if(init_thisnode(node, &joinaddr) == -1){

#ifdef DEBUGLOG
        LOG(&node->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }
#ifdef DEBUGLOG
    logNodeAdd(&node->addr,&node->addr);
#endif
    if(!introduceselftogroup(node, &joinaddr)){
        finishup_thisnode(node);
#ifdef DEBUGLOG
        LOG(&node->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/* 
Enqueue a message (buff) onto the queue env. 
*/
int enqueue_wrppr(void *env, char *buff, int size){    return enqueue((queue *)env, buff, size);}

/* 
Called by a member to receive messages currently waiting for it. 
*/
int recvloop(member *node){
    if (node->bfailed) return -1;
    else return MPrecv(&(node->addr), enqueue_wrppr, NULL, 1, &node->inmsgq); 
    /* Fourth parameter specifies number of times to 'loop'. */
}

