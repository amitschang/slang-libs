require("socket");
require("fork");
require("select");
require("pickle");
require("sysconf");

private variable TID=0; % master tid = 0
private variable BUFFER_SIZE=8192;
public variable _NUM_CPU; % forward declaration

typedef struct {
    tid,
    pid,
    fd,
    stat,
} Thread_Type;

typedef struct {
    pid, fd, % correspond to thread properties
    in, out,
    iplk, eplk,
    iglk, eglk,
    remote,
    is_remote,
    buf,
} Queue_Type;

private define new_thread(){
    TID++;
    variable thrd=@Thread_Type;
    thrd.tid=TID;
    thrd.fd=();
    thrd.pid=();
    return thrd;
}

private define _update_random_seed(){
    variable fun=eval("&srand");
    variable seed=[1239823123];
    if (stat_file("/dev/urandom")!=NULL){
	variable fh=fopen("/dev/urandom","rb");
	()=fread(&seed,ULong_Type,4,fh);
	fclose(fh);
    }
    (@fun)([_time,getpid,seed]);
}

public define _ncpu(){
   variable n = sysconf ("_SC_NPROCESSORS_ONLN");
   if (n != NULL)
     return n;

   % Sigh. Try a linux-specific guess.
   variable dir = "/sys/devices/system/cpu";
   if (NULL != stat_file (dir))
     return length(glob("$dir/cpu?"$));

   % We have at least 1 CPU
   return 1;
}

public define send_msg(){
    variable msg=pickle(());
    variable fd=();
    variable nn,n=0;
    %
    % First part will be 4 Byte unsigned integer indicating message
    % length of the message
    %
    variable mlen=bstrlen(msg);
    if (write(fd,pack(">I",mlen))<=0){
	throw IOError,"*threads-error*: could not initialize message ("+
	              errno_string(errno)+")";
    }
    else {
	while (n<mlen){
	    nn=write(fd,msg[[n:]]);
	    if (nn<0){
		%
		% This is quite bad, there is now partial data on the line
		% and surely will be corrupted
		%
		throw IOError,"*threads-error*: could not finish message "+
		              "transmission, probable data corruption ("+
		              errno_string(errno)+")";
	    }
	    n+=nn;
	}
    }
    return 1;
}

public define recv_msg(){
    variable fd=();
    variable buf,mlen,nn,n=0;
    variable msg="";
    %
    % First bytes will be the size of the message
    %
    if (read(fd,&mlen,sizeof_pack(">I"))<=0){
	throw IOError,"*threads-error*: could not initialize read message ("+
	              errno_string(errno) +")";
    }
    else {
	mlen=unpack(">I",mlen);
	while (n<mlen){
	    nn=read(fd,&buf,mlen-n); % try to read whatever part is unread
	    if (nn<0){
		throw IOError,"*threads-error*: could not complete message ( "+
		              errno_string(errno) +")";
	    }
	    n+=nn;
	    msg+=buf;
	}
	return unpickle(msg);
    }
}

private define thread_handle_int(sig){
    _exit(1);
}

public define thread(){
    %
    % Fisrt arg is function reference
    %
    variable args=__pop_list(_NARGS-1);
    variable fun=();
    variable s1,s2,retval;
    %
    % Set up a pipe so we can communicate results
    %
    (s1,s2)=socketpair(AF_UNIX,SOCK_STREAM,0);
    %
    % fork off the process. Ignore the child signal, we do not need to
    % reap the results, since the return value, communicated via the
    % socket channel, will indicate an error.
    %
    signal(SIGCHLD,SIG_IGN);
    %
    % PID will tell us if we are child or parent
    % 
    variable pid=fork();
    if (pid<0){
	throw AnyError,"could not create thread process";
    }
    else if (pid==0){
	signal(SIGINT,&thread_handle_int);
	variable err;
	try (err){
	    if (_featurep("rand") && is_defined("srand")){
		()=_update_random_seed;
	    }
	    %
	    % Allow functions that return arbitrary numbers of
	    % values. The stack is clear at this point, so make list
	    % of stkdepth length to return. The thread_join function
	    % should then push this list to return
	    %
	    (@fun)(__push_list(args));
	    retval=__pop_list(_stkdepth);
	    %
	    % The return value is actually a struct that can be probed
	    % to get the threads return status
	    % 
	    retval=struct{ err=0, value=retval };
	}
	catch AnyError: {
	    %
	    % Upon error, return the error structure which contains
	    % the error and message so main thread can rais infomative
	    % exception
	    % 
	    retval=struct{ err=1, value=err };
	}
	if (send_msg(s2,retval)!=1){
	    ()=close(s2);
	    _exit(0);
	}
	else if (retval==NULL){
	    _exit(0);
	}
	()=close(s2);
	_exit(1);
    }
    else {
	return new_thread(pid,s1);
    }
}

private define _thread_stop(){
    %
    % this is the blocking version of thread stop. Args 1,2 same as
    % thread_stop
    %
    variable dt=0.05;
    variable timeout=();
    variable tids=();
    if (typeof(tids)!=Array_Type) tids=[tids];
    variable exited=Int_Type[length(tids)];
    %
    % loop through threads, killing with SIGTERM
    %
    variable i;
    _for i (0,length(tids)-1,1){
	if (tids[i].stat==NULL){
	    ()=kill(tids[i].pid,SIGTERM);
	    ()=close(tids[i].fd);
	}
	else
	    exited[i]=1;
    }
    variable alive,stat;
    while (timeout>0){
	alive=where(exited==0);
	_for i (0,length(alive)-1,1){
	    stat=waitpid(tids[alive[i]].pid,WNOHANG);
	    if (stat==NULL)
	        exited[alive[i]]=1;
	    else if (stat.pid!=0)
		exited[alive[i]]=1;
	}
	if (length(alive)==0) break;
	sleep(dt);
	timeout-=dt;
    }
    %
    % Finally, really kill any ones left
    % 
    alive=where(exited==0);
    _for i (0,length(alive)-1,1){
	()=kill(tids[alive[i]].pid,SIGKILL);
	()=waitpid(tids[alive[i]].pid,0);
    }
}

public define thread_stop(){
    %
    % This function will terminate one or more threads. First input
    % argument is either a single thread or an array of threads to
    % terminate. The second (optional) argument is a timeout (in
    % seconds, default 10) before sending a harsher SIGKILL signal
    % (first attempt is a SIGTERM). The third argument is a bolean
    % telling whether to block (1) or not block (0, default). In
    % non-blocking mode, the function uses a thread to kill the
    % threads (ha!) and returns immediately - the calling program can
    % only be assured the threads will have been killed by timeout
    % seconds. In blocking mode the function will only return when all
    % threads have been successfully terminated. All threads will be
    % marked as completed with status of 1 even in blocking mode, to
    % prevent them from being joined later.
    % 
    variable blocking=0;
    variable timeout=10;
    if (_NARGS==3)
        blocking=();
    if (_NARGS>=2)
        timeout=();
    variable tids=();
    variable t;
    foreach t ([tids]){
	%
	% We are making sure to kill, and do certainly do not need the
	% fd anymore.
	% 
	t.stat=1;
	()=close(t.fd);
    }
    if (not blocking){
	t=thread(&_thread_stop,tids,timeout);
	%
	% We will not join this thread
	% 
	()=close(t.fd);
    }
    else {
	_thread_stop(tids,timeout);
    }
}

public define thread_join(){
    variable thread=();
    if (thread.stat!=NULL){
	error("Thread already joined");
    }
    variable ret=recv_msg(thread.fd);
    %
    % We don't need the waitpid, because we ignore the sigchld. the
    % success of the thread can be gleaned from the message recieved,
    % no need to get exit status
    % 
    ()=close(thread.fd);
    if (ret.err){
	variable err=ret.value;
	thread.stat=0;
	throw err.error,
	      sprintf("%s\n%s:%d:%s:%s",
	       err.descr,err.file,err.line,err.function,err.message);
    }
    __push_list(ret.value);
}

public define thread_select(){
    %
    % Select one or more threads from a list whose return value is
    % ready to read.
    % 
    variable threads=();
    if (typeof(threads)!=Array_Type || _typeof(threads)!=Thread_Type){
	return -1;
    }
    variable fds=FD_Type[length(threads)];
    variable valid=Int_Type[length(threads)];
    variable i;
    _for i (0,length(threads)-1,1){
	fds[i]=threads[i].fd;
	if (threads[i].stat==NULL && threads[i].pid!=NULL){
	    valid[i]=1;
	}
    }
    if (length(where(valid==1))==0){
	return Int_Type[0];
    }
    variable s=select(fds[where(valid==1)],,,qualifier("timeout",-1));
    return where(valid==1)[s.iread];
}

public define thread_map(){
    variable alen,mylist={},targ=NULL,ready,i;
    variable args=__pop_list(_NARGS-2);
    variable func=();
    variable type=();
    variable nthr=qualifier("ncpu",_NUM_CPU);
    %
    % find the argument that indexes threads, this is the first
    % argument with a non-unity length
    % 
    _for i (0,length(args)-1,1){
	if (length(args[i])>1 && targ==NULL){
	    alen=length(args[i]);
	    targ=i;
	    list_append(mylist,NULL);
	}
	else {
	    list_append(mylist,args[i]);
	}
    }
    %
    % Make return array of proper type, thread list and mask that
    % tells us the status of threads operations. available is the
    % indeces of thread list that we are allowed to populate with new
    % threads.
    % 
    variable retarr=(type)[alen];
    variable threads=Thread_Type[alen];
    variable mask=Int_Type[alen];
    variable avail=where(mask==0);
    if (length(avail)>nthr){avail=avail[[:nthr-1]];}
    while (length(where(mask!=2))>0){
	_for i (0,length(avail)-1,1){
	    mylist[targ]=args[targ][avail[i]];
	    threads[avail[i]]=thread(func,__push_list(mylist));
	    %
	    % Mark this one as taken
	    % 
	    mask[avail[i]]=1;
	}
	ready=thread_select(threads);
	foreach i (ready){
	    if (type!=Null_Type){
		retarr[i]=thread_join(threads[i]);
	    }
	    else {
		thread_join(threads[i]);
	    }
	    %
	    % Mark this one as complete
	    % 
	    mask[i]=2;
	}
	avail=where(mask==0);
	if (length(avail)>(length(ready))){
	    avail=avail[[:length(ready)-1]];
	}
    }
    return retarr;
}

private define queue_handler(){
    variable queue=();
    variable buf,sel,i,np=0,ng=0,host,port;
    variable rput={},rget={};
    if (typeof(queue.buf)!=List_Type){
	queue.buf={};
    }
    variable qfds=[queue.in,queue.iglk,queue.iplk];
    if (typeof(queue.remote)==FD_Type){
	qfds=[qfds,queue.remote];
    }
    forever {
	%
	% Monitor the "lock" connections, one manages data in, one
	% data out in order to provide effective concurrency. A lock
	% request will be a single byte recived, per request a single
	% byte will be replied, the reciever (can be only one) will
	% then be the sole actioner on the connection until done.
	%
	sel=select(qfds,,,1);
	%
	% Poll for the parent process, exit if they have exited. The 1
	% seconds above seems to eliminate cpu hogging, but should be
	% fast enough. Until I figure out a better way to exit...
	%
	if (getppid()==1){
	    foreach i (qfds){
		close(i);
	    }
	    exit(0);
	}
	foreach i (sel.iread){
	    if (i==1){
		()=read(queue.iglk,&buf,BUFFER_SIZE);
		ng+=count_byte_occurances(buf,'1');
		ng-=count_byte_occurances(buf,'0');
	    }
	    else if (i==2){
		()=read(queue.iplk,&buf,BUFFER_SIZE);
		np+=count_byte_occurances(buf,'1');
		np-=count_byte_occurances(buf,'0');
	    }
	    else if (i==3){
		qfds=[qfds,accept(queue.remote,&host,&port)];
	    }
	    else if (i>3){
		try {
		    if (recv_msg(qfds[i])=="PUT"){
			list_append(rput,i);
		    }
		    else {
			list_append(rget,i);
		    }
		}
		catch IOError: {
		    qfds=[qfds[[0:i-1]],qfds[[i+1:]]];
		}		    
	    }
	}
	%
	% Now hand out acquires until no more lock requests
	%
	for (i=np;i>0;i--){
	    ()=write(queue.iplk,"1");
	    buf=recv_msg(queue.in);
	    list_append(queue.buf,buf);
	    np--;
	}
	%
	% remote puts
	%
	while (length(rput)>0){
	    buf=recv_msg(qfds[list_pop(rput)]);
	    list_append(queue.buf,buf);
	}
	for (i=ng;i>0;i--){
	    if (length(queue.buf)>0){
		()=write(queue.iglk,"1");
		send_msg(queue.in,list_pop(queue.buf));
		ng--;
	    }
	}
	%
	% remote gets
	%
	for (i=0;i<length(rget);i++){
	    if (length(queue.buf)>0){
		send_msg(qfds[list_pop(rget,i)],list_pop(queue.buf));
	    }
	}
    }
}
	    
public define queue(){
    variable queue=@Queue_Type;
    %
    % Communications interface. There is a single data stream and one
    % lock management stream for each of GET and PUT operations.
    %
    % If we're meant to accept remote data, create a listening socket,
    % the "remote" qualifier should be a list of host,port
    %
    variable r=qualifier("remote",NULL);
    if (typeof(r)==List_Type){
	queue.remote=socket(AF_INET,SOCK_STREAM,0);
	bind(queue.remote,r[0],r[1]);
	listen(queue.remote,5);
    }
    (queue.in,queue.out)=socketpair(AF_UNIX,SOCK_STREAM,0);
    (queue.iglk,queue.eglk)=socketpair(AF_UNIX,SOCK_STREAM,0);
    (queue.iplk,queue.eplk)=socketpair(AF_UNIX,SOCK_STREAM,0);
    %
    % Start a queue manager thread, ignore ret val
    %
    variable t=thread(&queue_handler,queue);
    queue.pid=t.pid;
    queue.fd=t.fd;
    queue.is_remote=0;
    return queue;
}

public define queue_connect(){
    %
    % Connect to a REMOTE queue. No need to use this for local queue,
    % in that case just pass the queue object
    %
    variable port=();
    variable host=();
    variable q=@Queue_Type;
    q.out=socket(AF_INET,SOCK_STREAM,0);
    connect(q.out,host,port);
    q.is_remote=1;
    return q;
}

private define queue_lock(){
    variable lock_fd=();
    variable junk;
    if (write(lock_fd,"1")!=1){
	throw IOError,"Could not request lock";
    }
    else {
	()=read(lock_fd,&junk,BUFFER_SIZE);
	return;
    }
}

public define queue_put(){
    variable data=();
    variable queue=();
    if (queue.is_remote){
	if (send_msg(queue.out,"PUT")){
	    return send_msg(queue.out,data);
	}
	else {
	    return 0;
	}
    }
    queue_lock(queue.eplk);
    return send_msg(queue.out,data);
}

public define queue_get(){
    variable queue=();
    if (queue.is_remote){
	()=send_msg(queue.out,"GET");
    }
    else {
	queue_lock(queue.eglk);
    }
    return recv_msg(queue.out);
}

_NUM_CPU=_ncpu;

provide("threads");
