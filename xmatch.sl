#!/usr/bin/env slsh

require("cmdopt");
require("csv");
require("fits");
require("threads");

%
% Forward declarations
%
eval("define _cfunc(){}","_xmatch");
private define iterator();
private define stepiter();

private define stepiter(iters,lens,index){
    if (iters[index]<lens[index]-1){
	iters[index]=iters[index]+1;
	return iters;
    }
    iters[index]=0;
    return stepiter(iters,lens,index+1);
}

private define iterator(data,lens,pairs){
    variable i,j,m,c,p,x;
    variable args;
    if (length(@pairs)==1){
	m=_xmatch->_cfunc(get_struct_field(data,"A"));
	(@pairs)[0]=where(m==1);
	return;
    }
    variable iters=Integer_Type[length(@pairs)-1];
    %
    % Need to step the iterator before processing, set so that after
    % stepping we are at 0
    %
    iters[0]--;
    %
    % The number of times we have to do this looping is equal to the
    % product of lengths of all but the last item
    %
    variable loops=int(prod(lens[[:-2]]));
    loop (loops){
	iters=stepiter(iters,lens,0);
	args={};
	for (i=0;i<length(lens)-1;i++){
	    % c=char('A'+i);
	    x=get_struct_field(data,char('A'+i));
	    if (typeof(x)==Struct_Type){
		list_append(args,struct_filter(x,iters[i];copy));
	    }
	    else {
		list_append(args,x[iters[i]]);
	    }
	}
	list_append(args,get_struct_field(data,char('A'+length(lens)-1)));
	m=where(_xmatch->_cfunc(__push_list(args))==1);
	for (i=0;i<length(iters);i++){
	    (@pairs)[i]=[(@pairs)[i],Integer_Type[length(m)]+iters[i]];
	}
	(@pairs)[i]=[(@pairs)[i],m];
    }
}

define _xmatch() {% expr,data,...
    % 
    % See documentation for xmatch for a description of the
    % arguments. This is the underlying function that does the
    % matching. The return value is a list of indeces for matching row
    % combinations. The length of this list is the same as the number
    % of input data structures. The lengths of each index array is
    % always the same and is equal to the number of cross matches
    % (which may be 0). The Nth index of each array in the returned
    % list correspond to the row in that data structure that matched
    % the condition in all inpt data.
    %
    variable i,j,c,m,x;
    variable data=NULL;%Assoc_Type[Struct_Type];
    variable dlis={};
    variable lens=Integer_Type[_NARGS-1];
    variable pairs={};
    variable args="";
    for (i=_NARGS-2;i>=0;i--){
	c=char('A'+i);
	x=();
	data=struct_combine(data,c);
	set_struct_field(data,c,x);
	if (typeof(x)==Struct_Type){
	    lens[i]=length(get_struct_field(x,
	      get_struct_field_names(x)[0]));
	}
	else {
	    lens[i]=length(x);
	}
	args=char('A'+i)+","+args;
	list_append(pairs,Integer_Type[0]);
    }
    variable expr=();
    %%
    %% Define the comparison function dynamically
    %%
    eval(sprintf("define _cfunc(%s){ return %s; };",args[[:-1]],expr),"_xmatch");
    iterator(data,lens,&pairs);
    return pairs;
}

private define _xmatch_thread() {
    variable args=__pop_list(_NARGS-3);
    variable expr=();
    variable nthr=();
    variable indx=();
    variable l,mn,mx;
    if (is_struct_type(args[0]))
	l=length(get_struct_field(args[0],get_struct_field_names(args[0])[0]));
    else
        l=length(args[0]);
    mn=indx*l/nthr;
    mx=(indx+1)*l/nthr-1;
    if (indx==(nthr-1)) mx=l-1;
    if (is_struct_type(args[0]))
        struct_filter(args[0],[mn:mx]);
    else
        args[0]=args[0][[mn:mx]];
    variable ret=_xmatch(expr,__push_list(args));
    ret[0]+=mn;
    return ret;
}

define xmatch() {
    variable i,j,_tmp,ret;
    variable args=__pop_list(_NARGS);
    variable ncpu=qualifier("ncpu",_NUM_CPU);
    if (not qualifier_exists("nothreads") && ncpu > 1){
	variable thr=Thread_Type[_NUM_CPU];
	_for i (0,_ncpu-1,1){
	    thr[i]=thread(&_xmatch_thread,i,_NUM_CPU,__push_list(args));
	}
	ret=thread_join(thr[0]);
	_for i (1,_NUM_CPU-1,1){
	    _tmp=thread_join(thr[i]);
	    if (_tmp==NULL){
		error("Thread returned error status, please check conditional");
	    }
	    _for j (0,length(ret)-1,1){
		ret[j]=[ret[j],_tmp[j]];
	    }
	}
    }
    else {
	ret=_xmatch(__push_list(args));
    }
    if (length(ret[0])==0){
	return NULL;
    }
    return ret;
}

define xmatch_sky() {
    %
    % A specialized verion of xmatch that does sky positions,
    % optionally will return only the closest and the distance. The
    % first arg is the tolerence to match to
    %
    variable args=__pop_list(_NARGS);
    variable letters=array_map(String_Type,&char,['A':'A'+_NARGS-2]);
    variable expr="sqrt(("+strjoin(letters,".ra-")+".ra)^2 + ("+
                       strjoin(letters,".dec-")+".dec)^2)<"+
                  sprintf("%f",args[0]);
    vmessage("the expr is: %s",expr);
    return xmatch(expr,__push_list(args[[1:]]));
}

define xmatch_files() {
    %
    % This is a very simple cross matcher for files using the xmatch
    % function. it only can take files of the same format (since
    % qualifiers are passed in whole). A potential use is to apply an
    % arbitrary (set of) filter(s) upon reading in a single file.
    %
    variable args=__pop_list(_NARGS-1);
    variable expr=();
    variable data={};
    variable i;
    for (i=0;i<length(args);i++){
	list_append(data,csv_readcol(args[i];;__qualifiers()));
    }
    return xmatch(expr,__push_list(data));
}

define exit_usage() {
    fprintf(stdout,"Usage: %s [options] EXPR table[,table,...]\n",__argv[0]);
    exit();
}

define help_usage() {
    fprintf(stdout,"Usage: %s [options] EXPR table[,table,...]\n",__argv[0]);
    variable help="\n"+
    "  Cross match any number of tables based on arbitrary logical\n"+
    "  expression EXPR. Expression refers to the tables by the order\n"+
    "  they appear in the command line via letters A-Z. Columns are\n"+
    "  referred to by letter{dot}column_name such that:\n"+
    "\n"+
    "     ((A.ra-B.ra)^2+(A.dec-B.dec)^2)^0.5 < 0.01\n"+
    "\n"+
    "  would be a valid expression if the columns ra and dec exist in the\n"+
    "  first two tables specified on the command line\n"+
    "\n"+
    "Options:\n"+
    "    --help            show this message\n"+
    "    --delim           single or comma separated list of delimiters\n"+
    "    --headers         boolean, single or comma separated list\n"+
    "                      0 if file has no data header, 1 if present\n"+
    "";
    fprintf(stdout,help);
    exit();
}

define slsh_main() {
    %
    % A command line cross matcher. Arguments required are the
    % expression and any number of data tables
    %
    variable delims=NULL;
    variable headers=NULL;
    variable opts=cmdopt_new();
    opts.add("help",&help_usage);
    opts.add("delim",&delims;type="str");
    opts.add("headers",&headers;type="str");
    variable iend=opts.process(__argv,1);
    if (__argc-iend<2){
	exit_usage();
    }
    if (delims!=NULL){
	delims=strchop(delims,',',0);
	delims=int(delims);
    }
    if (headers!=NULL){
	headers=strchop(headers,',',0);
	headers=atoi(headers);
    }
    variable expr=__argv[iend];
    variable inputs=__argv[[iend+1:]];
    variable data={};
    variable i,di,quals;
    for (i=0;i<length(inputs);i++){
	%
	% read in data from files
	%
	if (path_extname(inputs[i])!="" &&
	    path_extname(inputs[i])[[:3]] ==".fit"){
	    list_append(data,fits_read_table(inputs[i]));
	}
	else {
	    quals=NULL;
	    if (delims!=NULL){
		if (length(delims)>=i+1) di=i;
		else if (length(delims)==1) di=0;		
		quals=struct_combine(quals,struct{delim=delims[di]});
	    }
	    if (headers!=NULL){
		if (length(headers)>=i+1 && headers[i]){
		    quals=struct_combine(quals,"has_header");
		}
		else if (headers[0]){
		    quals=struct_combine(quals,"has_header");
		}
	    }
	    else {
		quals=struct_combine(quals,"has_header");
	    }
	    quals=struct_combine(quals,struct{type='d'});
	    list_append(data,csv_readcol(inputs[i];;quals));
	}
    }
    variable out=xmatch(expr,__push_list(data));
    if (out!=NULL){
	%
	% assume the output format takes the delim of the first
	% argument if specified
	%
	if (delims!=NULL)
	    csv_writecol(stdout,out;delim=delims[0]);
	else
	    csv_writecol(stdout,out);
    }
}
