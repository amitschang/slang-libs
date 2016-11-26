private variable SL_PICKLE_START='\x80';
private variable SL_PICKLE_ARRAY='\x81';
private variable SL_PICKLE_IPACK='\x82';
private variable SL_PICKLE_STRNG='\x83';
private variable SL_PICKLE_STRUC='\x84';
private variable SL_PICKLE_NULLT='\x85';
private variable SL_PICKLE_ILIST='\x86';
private variable SL_PICKLE_SARRY='\x87';
private variable SL_PICKLE_ENDIT='\x89';
private variable SL_PICKLE_PACKS=struct{
    Double_Type="d",   d=Double_Type,
    Long_Type="l",     l=Long_Type,
    Integer_Type="i",  i=Integer_Type,
    Short_Type="h",    h=Short_Type,
    UChar_Type="c",    c=UChar_Type,
    Float_Type="f",    f=Float_Type,
    Float32_Type="F",  F=Float32_Type,
    Float64_Type="D",  D=Float64_Type,
    Int16_Type="j",    j=Int16_Type,
    Int32_Type="k",    k=Int32_Type,
    Int64_Type="q",    q=Int64_Type,
    LLong_Type="m",    m=LLong_Type,
    UInt16_Type="J",   J=UInt16_Type,
    UInt32_Type="K",   K=UInt32_Type,
    UInt64_Type="Q",   Q=UInt64_Type,
    UInteger_Type="I", I=UInteger_Type,
    ULLong_Type="M",   M=ULLong_Type,
    ULong_Type="L",    L=ULong_Type,
    UShort_Type="H",   H=UShort_Type,
};
private variable SL_INDEX_SIZEOF=sizeof_pack("I");

private define pickle_item();

private define pickle_scalar(){
    variable obj=();
    variable msg="";
    if (__is_numeric(obj)){
	msg+=array_to_bstring(SL_PICKLE_IPACK)+
	     get_struct_field(SL_PICKLE_PACKS,string(_typeof(obj)));
	msg+=pack(">"+
	get_struct_field(SL_PICKLE_PACKS,string(_typeof(obj))),
	obj);
    }
    else if (typeof(obj)==String_Type||typeof(obj)==BString_Type){
	msg+=array_to_bstring(SL_PICKLE_STRNG);
	msg+=pack(">I",bstrlen(obj));
	msg+=obj;
    }
    else {
	msg+=array_to_bstring(SL_PICKLE_NULLT);
    }
    return msg;
}

private define pickle_string_array(){
    variable obj=();
    variable msg;
    msg=strjoin(array_map(String_Type,&str_quote_string,obj,",",'\\'),",");
    msg=array_to_bstring(SL_PICKLE_SARRY)+pack(">I",strlen(msg))+msg;
    return msg;
}

private define pickle_array(){
    variable obj=();
    variable ax,i;
    variable msg=array_to_bstring(SL_PICKLE_ARRAY);
    %
    % pack the shape first
    %
    ax=array_shape(obj);
    msg+=pack(">I",length(ax));
    msg+=pack(">I"+string(length(ax)),ax);
    if (__is_numeric(obj)){
	msg+=array_to_bstring(SL_PICKLE_IPACK)+
	     get_struct_field(SL_PICKLE_PACKS,string(_typeof(obj)));
	msg+=pack(">"+
	get_struct_field(SL_PICKLE_PACKS,string(_typeof(obj)))+
	string(length(obj)),
	obj);
    }
    else if (_typeof(obj)==String_Type){
	msg+=pickle_string_array(obj);
    }
    else {
	foreach i (obj){
	    msg+=pickle_item(i);
	}
    }
    return msg;
}

private define pickle_list(){
    variable obj=();
    variable i;
    variable msg=array_to_bstring(SL_PICKLE_ILIST);
    msg+=pack(">I",length(obj));
    foreach i (obj){
	msg+=pickle_item(i);
    }
    return msg;
}

private define pickle_struct(){
    variable obj=();
    variable key;
    variable msg=array_to_bstring(SL_PICKLE_STRUC);
    variable keys=get_struct_field_names(obj);
    msg+=pack(">I",strlen(strjoin(keys,",")));
    msg+=strjoin(keys,",");
    foreach key (keys){
	msg+=pickle_item(get_struct_field(obj,key));
    }
    return msg;
}

private define pickle_item(){
    variable obj=();
    variable msg="";
    switch (typeof(obj)){ 
	case Array_Type:
	msg+=pickle_array(obj);
    }{
	case Struct_Type:
	msg+=pickle_struct(obj);
    }{
	case List_Type:
	msg+=pickle_list(obj);
    }{
	msg+=pickle_scalar(obj);
    }
    return msg;
}

public define pickle(){
    variable obj=();
    variable msg=array_to_bstring(SL_PICKLE_START);
    %
    % next indicate the type of object this is
    %
    msg+=pickle_item(obj);
    if (_NARGS==2){
	variable fh=fopen((),"w");
	()=fwrite(msg,fh);
	()=fclose(fh);
	return;
    }
    return msg;
}

%
% Unpickle routines. They should all take a reference to the message
% and the pos. It is very important that they leave the position at
% the TYPE indicator of the next part. So that the call the
% unpickle_type will know which function to call
%

private define unpickle_type();

private define unpickle_string(){
    variable pos=();
    variable msg=();
    variable slen=unpack(">I",(@msg)[[@pos:@pos+SL_INDEX_SIZEOF-1]]);
    (@pos)+=SL_INDEX_SIZEOF;
    variable data=string((@msg)[[@pos:@pos+slen-1]]);
    (@pos)+=slen;
    return data;
}

private define unpickle_packed(){
    variable len=1;
    if (_NARGS==3){
	len=();
    }
    variable pos=();
    variable msg=();
    variable dlen=sizeof_pack(char((@msg)[@pos])+string(len));
    variable data;
    if (dlen==0){
	data=get_struct_field(SL_PICKLE_PACKS,char((@msg)[@pos]))[0];
    }
    else {
	data=unpack(">"+char((@msg)[@pos])+string(len),(@msg)[[@pos+1:@pos+dlen]]);
    }
    @pos+=dlen+1;
    return data;
}

private define unpickle_string_array(){
    variable pos=();
    variable msg=();
    variable slen=unpack(">I",(@msg)[[@pos:@pos+SL_INDEX_SIZEOF-1]]);
    @pos+=SL_INDEX_SIZEOF;
    variable data=array_map(String_Type,
      &strreplace,strchop((@msg)[[@pos:@pos+slen-1]],',','\\'),"\\,",",");
    @pos+=slen;
    return data;
}
    
private define unpickle_array(){
    variable pos=();
    variable msg=();
    variable data;
    variable dlen=unpack(">I",(@msg)[[@pos:@pos+SL_INDEX_SIZEOF-1]]);
    variable dstart=@pos+SL_INDEX_SIZEOF+dlen*SL_INDEX_SIZEOF;
    variable dims=unpack(">I"+string(dlen),(@msg)[[@pos+SL_INDEX_SIZEOF:dstart-1]]);
    @pos=dstart;
    %
    % if the next bit is pack indicator, the entire array is packed
    % data
    %
    if ((@msg)[@pos]==SL_PICKLE_IPACK){
	@pos++;
	data=unpickle_packed(msg,pos,int(prod(dims)));
    }
    else if ((@msg)[@pos]==SL_PICKLE_SARRY){
	@pos++;
	data=unpickle_string_array(msg,pos);
    }
    else {
	%
	% determine the type of first item, this will be type of
	% entire array
	%
	variable item,i;
	()=unpickle_type(msg,pos,&item);
	data=(typeof(item))[int(prod(dims))];
	data[0]=item;
	for (i=1;i<prod(dims);i++){
	    ()=unpickle_type(msg,pos,&item);
	    data[i]=item;
	}
    }
    %
    % now reshape to original spec
    %
    reshape(data,dims);
    return data;
}

private define unpickle_list(){
    variable pos=();
    variable msg=();
    variable llen=unpack(">I",(@msg)[[@pos:@pos+SL_INDEX_SIZEOF-1]]);
    variable i,type;
    @pos=@pos+SL_INDEX_SIZEOF;
    variable data={};
    variable item;
    for (i=0;i<llen;i++){
	()=unpickle_type(msg,pos,&item);
	list_append(data,item);
    }
    return data;
}

private define unpickle_struct(){
    variable pos=();
    variable msg=();
    variable klen=unpack(">I",(@msg)[[@pos:@pos+SL_INDEX_SIZEOF-1]]);
    (@pos)+=SL_INDEX_SIZEOF;
    variable keys=strchop((@msg)[[@pos:@pos+klen-1]],',',0);
    (@pos)+=klen;
    variable data=struct_combine(keys);
    variable key,val,type;
    foreach key (keys){
	()=unpickle_type(msg,pos,&val);
	set_struct_field(data,key,val);
    }
    return data;
}

private define unpickle_type(){
    variable tmp=();
    variable pos=();
    variable msg=();
    variable data,type=(@msg)[@pos];
    @pos++;
    switch (type){
	case SL_PICKLE_ARRAY:
	data=unpickle_array(msg,pos);
    }{
	case SL_PICKLE_STRUC:
	data=unpickle_struct(msg,pos);
    }{
	case SL_PICKLE_IPACK:
	data=unpickle_packed(msg,pos);
    }{
	case SL_PICKLE_STRNG:
	data=unpickle_string(msg,pos);
    }{
	case SL_PICKLE_ILIST:
	data=unpickle_list(msg,pos);
    }{
	case SL_PICKLE_NULLT:
	data=NULL;
    }{
	return 0;
    }
    @tmp=data;
    return 1;
}

public define unpickle(){
    variable msg=();
    if (typeof(msg)==String_Type){
	variable fh=fopen(msg,"r");
	fread_bytes(&msg,stat_file(msg).st_size,fh);
	()=fclose(fh);
    }
    if (typeof(msg)!=BString_Type){
	throw ApplicationError,"*pickle-error*: Expecting BString type";
    }
    if (msg[0]!=abs(int(SL_PICKLE_START))){
	throw IOError,"*pickle-error*: String does not appear to be a pickled object";
    }
    variable data={};
    variable pos=1;
    variable tmp;
    variable i=0;
    try {
	%
	% will pass around msg and pos as refs
	%
	while (pos<bstrlen(msg)){
	    if (unpickle_type(&msg,&pos,&tmp)){
		list_append(data,tmp);
	    }
	    if (msg[pos]==SL_PICKLE_START){
		pos++;
	    }
	    else {
		break;
	    }
	}
    }
    catch AnyError: {
	throw ParseError,"*pickle-error*: Bad format for pickled string";
    }
    __push_list(data);
    return;
}

provide("pickle");
