// gcc -Wall -fno-strict-aliasing -static -s -Os -pthread -o tm tm.c -lev -lm
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/wait.h>
#include <pthread.h>
#include <string.h>
#include <ev.h>
#include <errno.h>
#include <sys/socket.h>
#include <resolv.h>
#include <sys/uio.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <utime.h>
#include <ctype.h>
#include <dirent.h>
#include <ifaddrs.h>
#include <limits.h>
#include <net/if.h> 
#include <sys/ioctl.h>
#include <linux/if_link.h>
#include <netdb.h>
#include <sys/resource.h>



#define STR(s) #s
#define XSTR(s) STR(s)

#define BUF_SIZE 4096           // bufsize (must be > IPLEN + IDLEN*2 + 16 )
#define MAXDATA 256             // maximum length of a data line
#define SENDER_BUFSIZ 512       // sender thread buffer size
#define DATADIR "/tmp/tm_data/"
#define TMPMASK "/tmp/tmtmp.XXXXXX"
#define LOGF "/tmp/tm.log"
#define MAXAGE 3600             // data will be removed after MAXAGE seconds if not refreshed
#define BUSPORT 7697            // broadcast bus port
#define INPUTPORT 7698          // leader sensor input tcp port and vote input udp port
#define LOCALPORT 7699          // local sensor input fwd-ed to LEADER_IP:INPUTPORT

#define PV 'a'                  // protocol version
#define BUSTMO 20               // minimum timeout for bus inactivity and start voting session
#define VOTETMO 20              // voting timeout
#define HEARTBEAT 1             // 4 leader sequences in every HEARTBEAT seconds
#define GL_1 'G'
#define GL_2 'L'
#define GLOBALID "xxxxxxxx"
#define GLDATA "GL00" GLOBALID  // master data name (leader ip and id)
#define T_UDP 'u'
#define T_TCP 't'
#define ADDR_BROADCAST "255.255.255.255"
#define WTFMSG "WTF"
#define IPLEN (4*3+3)           // max length of decimal coded ip address
#define IDLEN (8)
  
#define ROLE_READER 0
#define ROLE_VOTER  1
#define ROLE_LEADER 2



static void udp_bus_cb(struct ev_loop *loop, ev_io *w, int revents);



struct tcp_data
{
  void (*cb)(EV_P_ ev_io *,int);
};



static ev_timer timeout_watcher;
static ev_timer heartbeat_watcher;
static struct ev_loop *loop;
static int pipew=-1;
static int pipef=-1;
static int role=ROLE_READER;
static char nodeid[IDLEN+1]="0000host";   // <POWER><HOSTNAME> (8bytes)
static char ip_self[IPLEN+1];             // own ip address facing to default gateway
static char leaderid[IDLEN+1];
static char leaderip[IPLEN+1];
static char prevleaderid[IDLEN+1];
static char prevleaderip[IPLEN+1];
static int quit=0;

static int udp_input_sd=-1;
static int udp_bus_sd=-1;
static int tcp_local_sd=-1;
static int tcp_input_sd=-1;

static ev_io udp_input_watcher;
static ev_io udp_bus_watcher;
static ev_io tcp_local_watcher;
static ev_io tcp_input_watcher;



// Paul Larson hash
static unsigned int hash(const char* s, unsigned int seed)
{
  unsigned int hash=seed;
    
  while(*s) hash=hash*101+*s++;

  return(hash);
}


static void primary_ip(char* buffer, size_t buflen)
{
  const char *dnsip="8.8.8.8";
  struct sockaddr_in srv;
  int s;
  struct sockaddr_in name;
  socklen_t namelen=sizeof(name);
  
  if(buflen>=16&&buffer!=NULL)
  {
    buffer[0]='\0';
    if((s=socket(AF_INET,SOCK_DGRAM,0))>=0)
    {
      memset(&srv,0,sizeof(srv));
      srv.sin_family=AF_INET;
      srv.sin_addr.s_addr=inet_addr(dnsip);
      srv.sin_port=htons(53);
      if(   -1!=connect(s,(const struct sockaddr*)&srv,sizeof(srv))
         && -1!=getsockname(s,(struct sockaddr*)&name,&namelen))
        inet_ntop(AF_INET,&name.sin_addr,buffer,buflen);
      close(s);
    }
  }
}


static int send_udp(char *addr, int port, char *msg, int len)
{
  int ret=-1;
  struct sockaddr_in si;
  int s,rc=1;
  int bcen=0;
  
  if(NULL!=msg&&len>0&&NULL!=addr&&port>0&&port<0x8000)
  {
    if((s=socket(AF_INET,SOCK_DGRAM,IPPROTO_UDP))!=-1)
    {
      if(strcmp(addr,ADDR_BROADCAST)==0) bcen=1;
      if(bcen==0||0==setsockopt(s,SOL_SOCKET,SO_BROADCAST,&bcen,sizeof(bcen)))
      {
        memset((char *)&si,0,sizeof(si));
        si.sin_family=AF_INET;
        si.sin_port=htons(port);
        if(bcen!=0) si.sin_addr.s_addr=htonl(INADDR_BROADCAST);
        else rc=inet_aton(addr,&si.sin_addr);
        if(rc!=0)
        {
          if(sendto(s,(const void *)msg,(size_t)len,0,(const struct sockaddr *)&si,sizeof(si))!=-1) ret=0;
          else fprintf(stderr,"%s: sendto failed %s:%d '%s'\n",__func__,addr,port,msg);
        }
        else fprintf(stderr,"%s: aton failure %s:%d\n",__func__,addr,port);
      }
      else fprintf(stderr,"%s: cannot enable broadcast\n",__func__);
      close(s);
    }
    else fprintf(stderr,"%s: cannot create socket\n",__func__);
  }
  else fprintf(stderr,"%s: invalid input\n",__func__);
  
  return(ret);
}


static int send_tcp(char *addr, int port, char *msg, int len)
{
  int ret=-1;
  struct sockaddr_in si;
  int s;
  
  if(NULL!=msg&&len>0&&NULL!=addr&&port>0&&port<0x8000)
  {
    if((s=socket(PF_INET,SOCK_STREAM,IPPROTO_TCP))>=0)
    {
      memset((char *)&si,0,sizeof(si));
      si.sin_family=AF_INET;
      si.sin_port=htons(port);
      if(inet_aton(addr,&si.sin_addr)!=0)
      {
        if(connect(s,(struct sockaddr *)&si,sizeof(si))>=0)
        {
          if(send(s,msg,len,0)==len) ret=0;
          close(s);
        }
        else fprintf(stderr,"%s: cannot connect to %s\n",__func__,addr);
      }
      else fprintf(stderr,"%s: aton failure %s:%d\n",__func__,addr,port);
    }
    else fprintf(stderr,"%s: cannot create socket\n",__func__);
  }
  else fprintf(stderr,"%s: invalid input\n",__func__);
  
  return(ret);
}


/*
 * sender protocol:
 *
 * u;192.168.1.12;112;7;Message
 * T A            P   L M
 *
 * T - type: u: udp
 *           t: tcp
 *           q: quit (no other data required)
 * A - addr: ip address in decimal notation
 * P - port: decimal
 * L - len: message length
 * M - message: message to send (len bytes including zero if needed)
 *
 */
static void *sender_thread(void *p)
{
  char buf[SENDER_BUFSIZ+1];
  int fd=(int)((long)p);
  char *ip,*msg,*n;
  int port,len,mode=0,l,st;

  setpriority(PRIO_PROCESS,0,1);
  while(1)
  {
    st=1;
    l=read(fd,buf,sizeof(buf));
    //printf("  senderth: read \"%s\"\n", buf);
    if(mode==0)
    {
      if(buf[0]=='q') break;
      // parse addr, port, len
      ip=&buf[2];
      if(NULL!=(n=strchr(ip,';')))
      {
        *n='\0';
        port=atoi(++n);
        if(NULL!=(n=strchr(n,';')))
        {
          len=atoi(++n);
          if(NULL!=(msg=strchr(n,';')))
          {
            msg++;
            // TODO: implement the buffer allocation for longer messages
            if(msg-buf+len<=l)
            {
              if(buf[0]=='u') st=send_udp(ip,port,msg,len);
              else if(buf[0]=='t') st=send_tcp(ip,port,msg,len);
              else fprintf(stderr,"unknown command char \"%c\": ",buf[0]);
            }
            else fprintf(stderr,"WARNING: too long message: %ld read only the first %d bytes: ",(long int)(msg-buf+len),l);
          }
          else fprintf(stderr,"missing message separator: ");
        }
        else fprintf(stderr,"missing length separator: ");
      }
      else fprintf(stderr,"missing ip separator: ");
      if(st!=0) fprintf(stderr,"st=%d '%s'\n",st,buf);
    }
  }
  pthread_exit(NULL);

  return(NULL);
}


static int write_file(int age, const char *name, const char *str)
{
  int ret=-1;
  char *nam;
  int fd,ln;
  char tmpnam[]=TMPMASK;
  struct utimbuf tms;
  
  setpriority(PRIO_PROCESS,0,2);
  if(name!=NULL)
  {
    if(NULL!=(nam=malloc(sizeof(DATADIR)+strlen(name)+1)))
    {
      strcpy(nam,DATADIR);
      strcat(nam,name);
      if(str!=NULL)
      {
        ln=strlen(str);
        if((fd=mkstemp(tmpnam))>=0)
        {
          if(ln==write(fd,str,ln))
          {
            close(fd);
            tms.actime=tms.modtime=time(NULL)-age;
            if(0==utime(tmpnam,&tms))
            {
              if(0==chmod(tmpnam,0666))
              {
                if(0==rename(tmpnam,nam)) ret=0;
                else fprintf(stderr,"%s: rename %s to %s failed\n",__func__,tmpnam,nam);
              }
              else fprintf(stderr,"%s: chmod failed\n",__func__);
            }
            else fprintf(stderr,"%s: utime failed\n",__func__);
          }
          else fprintf(stderr,"%s: write failed\n",__func__);
          if(ret!=0) unlink(tmpnam);
        }
        else fprintf(stderr,"%s: mkstemp failed\n",__func__);
      }
      else unlink(nam);
      free(nam);
    }
  }
  
  return(ret);
}


static int file_crate(const char *name, time_t timestamp, const char *data)
{
  int ret=-1;
  
  if(NULL!=name&&data!=NULL) ret=write_file(time(NULL)-timestamp,name,data);
  
  return(ret);
}


static int file_delete(const char *name)
{
  int ret=-1;
  
  if(name) ret=unlink(name);
  
  return(ret);
}


/*
 * file io protocol:
 *
 * c;1491024061;/tmp/tm_data/TC00ff33hurk;len;content...
 * t m          p                         l   c
 *
 * t - type: c: create
 *           d: delete
 *           q: quit
 * m - last modification time (0 for deletion)
 * p - path (cannot contain ';')
 * l - data length
 * c - content (can be empty)
 *
 */
static void *file_thread(void *p)
{
  char buf[SENDER_BUFSIZ+1];
  int fd=(int)((long)p);
  char *nm,*dt,*n;
  int len,mode=0,l,st;
  time_t ts;

  while(1)
  {
    st=1;
    l=read(fd,buf,sizeof(buf));
    if(mode==0)
    {
      if(buf[0]=='q') break;
      // parse timestamp, path and content
      n=&buf[2];
      ts=strtol(n,NULL,10);
      if(NULL!=(n=strchr(n,';')))
      {
        nm=++n;
        if(NULL!=(n=strchr(n,';')))
        {
          *n++='\0';
          len=atoi(n);
          if(NULL!=(n=strchr(n,';')))
          {
            dt=++n;
            // TODO: implement the buffer allocation for longer messages
            if(dt-buf+len<=l)
            {
              if(buf[0]=='c') st=file_crate(nm,ts,dt);
              else if(buf[0]=='d') st=file_delete(nm);
              else fprintf(stderr,"unknown command char \"%c\": ",buf[0]);
            }
            else fprintf(stderr,"WARNING: too long data: %ld read only the first %d bytes: ",(long int)(dt-buf+len),l);
          }
          else fprintf(stderr,"missing data separator");
        }
        else fprintf(stderr,"missing length separator");
      }
      else fprintf(stderr,"missing timestamp separator: ");
      if(st!=0) fprintf(stderr,"st=%d '%s'\n",st,buf);
    }
  }
  pthread_exit(NULL);

  return(NULL);
}


// request sending something to somewhere, schedule the request using the pipe which read by the sender thread
static int sender_add(char type, char *addr, int port, char *msg)
{
  int ret=-1;
  char *b;                  // u;192.168.1.12;112;7;Message
  char n[]="\n";
  int blen,len,mlen;
  
  if(pipew>0&&addr!=NULL&&port>0&&port<0x8000&&NULL!=msg)
  {
    mlen=strlen(msg);
    blen=strlen(addr)+5+mlen+5+5+2;
    if(NULL!=(b=malloc(blen)))
    {
      if(msg[mlen-1]=='\n') n[0]='\0';
      else n[0]='\n';
      snprintf(b,blen,"%c;%s;%d;%ld;%s%s",type,addr,port,(long int)mlen+(n[0]=='\0'?0:1),msg,n);
      len=strlen(b)+1;
      if(write(pipew,b,len)==len) ret=0;
      free(b);
    }
  }
  
  return(ret);
}


// request creating/deleting file, schedule the request using the pipe which read by the file thread
static int file_create_add(int age, const char *name, const char *data)
{
  int ret=-1;
  char *b;                 	// c;1491024061;/tmp/tm_data/TC00ff33hurk;len;content...
  int blen,len,dlen;
  
  if(pipef>0&&name!=NULL&&NULL!=data)
  {
    dlen=strlen(data);
    blen=strlen(name)+4+dlen+20+20+2;
    if(NULL!=(b=malloc(blen)))  // FIXME: use static buffers
    {
      snprintf(b,blen,"c;%ld;%s;%ld;%s",time(NULL)-age,name,(long int)dlen,data);
      len=strlen(b)+1;
      if(write(pipef,b,len)==len) ret=0;
      free(b);
    }
  }
  
  return(ret);
}


static int file_delete_add(const char *name)
{
  int ret=-1;
  char *b;                  // d;0;/tmp/tm_data/TC00ff33hurk;0;
  int blen,len;
  
  if(pipef>0&&name!=NULL)
  {
    blen=strlen(name)+4+3+1;
    if(NULL!=(b=malloc(blen)))  // FIXME: use static buffers
    {
      snprintf(b,blen,"d;0;%s;0;",name);
      len=strlen(b)+1;
      if(write(pipef,b,len)==len) ret=0;
      free(b);
    }
  }
  
  return(ret);
}


static void timeout_cb(EV_P_ ev_timer *w, int revents)
{
  fprintf(stderr,"timeout\n");
  ev_break(EV_A_ EVBREAK_ONE);
}


static int getfileage(const char *n, time_t now)
{
  int ret=0;
  struct stat st;
  
  if(NULL!=n)
  {
    if(0==lstat(n,&st))
    {
      if(now==0) now=time(NULL);
      ret=difftime(now,st.st_mtim.tv_sec);
    }
  }
  
  return(ret);
}


static void set_leader(char *nam)
{
  FILE *f;
  
  if(nam!=NULL&&NULL!=(f=fopen(nam,"w")))
  {
    // 10.0.1.7,ff6abana
    fprintf(f,"%s,%s",ip_self,nodeid);
    fclose(f);
  }
  strcpy(leaderip,ip_self);
  strcpy(leaderid,nodeid);
}


static int init_udp(int *sd, struct ev_loop *l, ev_io *w, int port, void (*cb)(EV_P_ ev_io *,int) )
{
  int ret=-1;
  struct sockaddr_in addr;
  const int enable=1;
  
  if(sd!=NULL&&loop!=NULL&&w!=NULL&&cb!=NULL&&port>0&&port<0x8000)
  {
    *sd=socket(PF_INET,SOCK_DGRAM,0);
    if(setsockopt(*sd,SOL_SOCKET,SO_REUSEADDR,&enable,sizeof(int))>=0)
    {
      bzero(&addr,sizeof(addr));
      addr.sin_family=AF_INET;
      addr.sin_port=htons(port);
      addr.sin_addr.s_addr=INADDR_ANY;
      if(bind(*sd,(struct sockaddr*)&addr,sizeof(addr))==0)
      {
        ev_io_init(w,cb,*sd,EV_READ);
        ev_io_start(l,w);
        ret=0;
      }
      else fprintf(stderr,"%s: bind failed\n",__func__);
    }
    else fprintf(stderr,"%s: setsockopt REUSEADDR failed\n",__func__);
  }
  else fprintf(stderr,"%s: invalid input\n",__func__);

  return(ret);
}


static void close_udp(int *sd, struct ev_loop *l, ev_io *w)
{
  if(NULL!=sd&&NULL!=l&&NULL!=w)
  {
    if(*sd!=-1)
    {
      ev_io_stop(l,w);
      close(*sd);
      *sd=-1;
    }
  }
}


static void heartbeat_cb(EV_P_ ev_timer *w, int revents)
{
  char pdu[BUF_SIZE];
  char nam[sizeof(DATADIR)+IDLEN+4];
  char age[]="0000";
  char *p;
  int c,len,a;
  
  pdu[0]='\0';
  if(pipew>0)
  {
    if(role==ROLE_VOTER)
    {
      // send voting request "?a<NODEID>,<IP>"
      snprintf(pdu,sizeof(pdu),"?%c%s,%s",PV,nodeid,ip_self);
      if(pdu[0]!='\0') sender_add(T_UDP,ADDR_BROADCAST,BUSPORT,pdu);
    }
    else if(role==ROLE_LEADER)
    {
      // send data to bus
      time_t now;
      FILE *fp;
      DIR *d;
      struct dirent *e;
      
      set_leader(DATADIR GLDATA);
      pdu[0]='+';
      pdu[1]=PV;
      pdu[2]='\0';
      p=&pdu[2];
      len=sizeof(pdu)-3;
      now=time(NULL);
      if(NULL!=(d=opendir(DATADIR)))
      {
        while((e=readdir(d)))
        {
          if((DT_REG==e->d_type||DT_UNKNOWN==e->d_type)&&e->d_name[0]!='.')
          {
            strcpy(nam,DATADIR);
            strcat(nam,e->d_name);
            if(len<IDLEN+4+1)
            {
              fprintf(stderr,"%s: buffer too small, skipping '%s'\n",__func__,e->d_name);
              continue;
            }
            a=getfileage(nam,now);
            if(a>MAXAGE)
            {
              file_delete_add(nam);
              continue;
            }
            // space
            *p=' '; len--; *++p='\0';
            // mod time
            c=snprintf(age,sizeof(age),"%04x",a);
            strcat(p,age);
            len-=c; p+=c;
            // basename
            strncpy(p,e->d_name,IDLEN+4);
            len-=IDLEN+4; p+=IDLEN+4; *p='\0';
            // content
            if(NULL!=(fp=fopen(nam,"rb")))
            {
              c=fread(p,1,(len<MAXDATA?len:MAXDATA),fp);
              if(c>0)
              {
                len-=c;
                p+=c;
                *p='\0';
              }
              else
              {
                fprintf(stderr,"%s: read error '%s' skipped\n",__func__,e->d_name);
                p-=IDLEN+4+4+1;
                len+=IDLEN+4+4+1;
              }
              fclose(fp);
            }
            else fprintf(stderr,"%s: file open error '%s'",__func__,e->d_name);
          }
        }
        closedir(d);
      }
      if(pdu[0]!='\0') sender_add(T_UDP,ADDR_BROADCAST,BUSPORT,pdu);
    }
  }
}


static void accept_tcp_cb(struct ev_loop *loop, struct ev_io *w, int revents)
{
  struct sockaddr_in client_addr;
  socklen_t client_len=sizeof(client_addr);
  int client_sd;
  struct ev_io *wc;
  struct tcp_data *td;
  int st=1;

  if(NULL!=(wc=(struct ev_io*)malloc(sizeof(struct ev_io))))
  {
    if(NULL!=w)
    {
      if(NULL!=(td=w->data)&&NULL!=td->cb)
      {
        if(!(EV_ERROR&revents))
        {
          if((client_sd=accept(w->fd,(struct sockaddr *)&client_addr,&client_len))>=0)
          {
            ev_io_init(wc,td->cb,client_sd,EV_READ);
            ev_io_start(loop,wc);
            st=0;
          }
          else fprintf(stderr,"%s: accept failed\n",__func__);
        }
        else fprintf(stderr,"%s: invalid event\n",__func__);
      }
      else fprintf(stderr,"%s: missing tcp_data field",__func__);
    }
    else fprintf(stderr,"%s: invalid watcher\n",__func__);
    if(st!=0) free(wc);
  }
  else fprintf(stderr,"%s: no memory\n",__func__);
}


static int init_tcp(int *sd, struct ev_loop *l, ev_io *w, int port, void (*cb)(EV_P_ ev_io *,int) )
{
  int ret=-1;
  struct sockaddr_in addr;
  const int enable=1;
  struct tcp_data *td;

  if(NULL!=sd&&NULL!=l&&NULL!=w&&cb!=NULL&&port>0&&port<0x8000)
  {
    if(NULL!=(td=malloc(sizeof(struct tcp_data))))
    {
      w->data=td;
      td->cb=cb;
      if((*sd=socket(PF_INET,SOCK_STREAM,0))>=0)
      {
        if(setsockopt(*sd,SOL_SOCKET,SO_REUSEADDR,&enable,sizeof(int))>=0)
        {
          bzero(&addr,sizeof(addr));
          addr.sin_family=AF_INET;
          addr.sin_port=htons(port);
          addr.sin_addr.s_addr=INADDR_ANY;
          if(bind(*sd,(struct sockaddr*)&addr,sizeof(addr))==0)
          {
            if(listen(*sd,4)>=0)
            {
              ev_io_init(w,accept_tcp_cb,*sd,EV_READ);
              ev_io_start(l,w);
            }
            else fprintf(stderr,"%s: listen failed\n",__func__);
          }
          else fprintf(stderr,"%s: cannot bind socket\n",__func__);
        }
        else fprintf(stderr,"%s: cannot set SO_REUSEADDR on socket\n",__func__);
      }
      else fprintf(stderr,"%s: cannot create socket\n",__func__);
    }
    else fprintf(stderr,"%s: no memory\n",__func__);
  }
  
  return(ret);
}


static void close_tcp(int *sd, struct ev_loop *l, ev_io *w)
{
  char buf[1];
  
  if(NULL!=sd&&NULL!=l&&NULL!=w)
  {
    if(*sd!=-1)
    {
      ev_io_stop(l,w);
      shutdown(*sd,1);
      while(read(*sd,buf,sizeof(buf))>0);
      close(*sd);
      *sd=-1;
      if(NULL!=w->data) free(w->data);
      w->data=NULL;
    }
  }
}


static int delete_old(int maxage)
{
  char nam[sizeof(DATADIR)+IDLEN+4];
  time_t now;
  DIR *d;
  struct dirent *e;

  now=time(NULL);
  if(NULL!=(d=opendir(DATADIR)))
  {
    while((e=readdir(d)))
    {
      if((DT_REG==e->d_type||DT_UNKNOWN==e->d_type)&&e->d_name[0]!='.')
      {
        strcpy(nam,DATADIR);
        strcat(nam,e->d_name);
        if(getfileage(nam,now)>maxage) file_delete_add(nam);
      }
    }
    closedir(d);
  }

  return(0);
}


// 0007HUEaff6abana-22-22-20-20-20-fe-fe-fe 
// 0000MS00xxxxxxxx10.0.1.8,ffe6ggma 
// 0243TC00ffe6ggma19.0C
// 012345678901234567
static int process_item(char *s, int dry)
{
  int ret=-1;
  int age;
  char a[5];
  char n[13];
  char *d;
  
  if(NULL!=s&&strlen(s)>=15)
  {
    strncpy(a,s,4);
    a[4]='\0';
    age=(int)strtol(a,NULL,16);
    strncpy(n,&s[4],12);
    n[12]='\0';
    d=&s[16];
    if(!dry) file_create_add(age,n,d);
    ret=0;
    if(strcmp(n,GLDATA)==0)
    {
      char *lid;
      if(NULL!=(lid=strchr(d,',')))
      {
        *lid++='\0';
        if(strlen(d)<=IPLEN&&strcmp(leaderip,d)!=0)
        {
          ret=1;
          strcpy(prevleaderip,leaderip);
          strcpy(prevleaderid,leaderid);
          strcpy(leaderip,d);
          if(strlen(lid)<=IDLEN) strcpy(leaderid,lid);
        }
      }
    }
  }
  
  return(ret);
}


static int process_line(char *buf, int dry)
{
  int ret=0,st;
  char *p,*e=NULL;
  
  for(p=buf;p!=NULL;p=e)
  {
    if(NULL!=(e=strchr(p,' '))) *e++='\0';
    else if(NULL!=(e=strchr(p,'\n'))) *e++='\0';
    if(strlen(p)>4&&-1!=(st=process_item(p,dry))) ret+=st;
  }
  
  return(ret);
}


static int getrank(const char *nid)
{
  int ret=-1;
  char r[]="0000";

  if(nid!=NULL)
  {
    strncpy(r,nid,4);
    ret=(int)strtol(r,NULL,16);
  }
  
  return(ret);
}


static void udp_input_cb(struct ev_loop *loop, ev_io *w, int revents)
{
  char buf[BUF_SIZE];
  struct sockaddr_in addr;
  int addr_len=sizeof(addr);
  socklen_t len;
  
  len=recvfrom(w->fd,buf,sizeof(buf)-1,0,(struct sockaddr*)&addr,(socklen_t *)&addr_len);
  if(role==ROLE_VOTER)
  {
    if(len>0)
    {
      buf[len]='\0';
      if(buf[0]=='!')
      {
        if(getrank(nodeid)<getrank(&buf[2]))
        {
          printf("%s: voting lost\n",__func__);
          role=ROLE_READER;
          ev_break(EV_A_ EVBREAK_ONE);
        }
      }
    }
  }
}


static void udp_bus_cb(struct ev_loop *loop, ev_io *w, int revents)
{
  static time_t lasthb=0;
  static time_t lastvoted=0;
  static char votedfor[IDLEN+1]={0};
  time_t now;
  char buf[BUF_SIZE];
  struct sockaddr_in addr;
  int addr_len=sizeof(addr);
  socklen_t len;

  len=recvfrom(w->fd,buf,sizeof(buf)-1,0,(struct sockaddr*)&addr,(socklen_t *)&addr_len);
  if(len>0)
  {
    buf[len-1]='\0';
    if(role==ROLE_READER||role==ROLE_VOTER)
    {
      if(role==ROLE_READER) ev_timer_again(loop,&timeout_watcher);
      now=time(NULL);
      if(buf[0]=='+')          // data (aka heartbeat)
      {
        if(process_line(&buf[2],0)>0)
        {
          // leaderip changed, check conditions
          if(difftime(now,lasthb)<BUSTMO)
          {
            // double leaders, send WTF to the weaker
            char wtfip[IPLEN+1];
            if(getrank(prevleaderid)>getrank(leaderid)) strcpy(wtfip,prevleaderip);
            else strcpy(wtfip,leaderip);
            sender_add(T_TCP,wtfip,INPUTPORT,WTFMSG);
          }
        }
        lasthb=now;
      }
      else if(buf[0]=='?')     // voting request
      {
        int voteage=difftime(now,lastvoted);
        if(       voteage>VOTETMO
            || (  voteage<VOTETMO && strncmp(votedfor,&buf[2],sizeof(votedfor))==0 )
            || ( (voteage<VOTETMO || lastvoted==0) && getrank(&buf[2])>getrank(votedfor) )
          )
        {
          int votelen;
          char *vote;
          votelen=strlen(buf)+IDLEN+2;
          if(NULL!=(vote=malloc(votelen)))
          {
            snprintf(vote,votelen,"!%s,%s",&buf[1],nodeid);
            printf("vote: '%s'\n",vote);
            sender_add(T_UDP,ADDR_BROADCAST,INPUTPORT,vote);
            free(vote);
            lastvoted=now;
            bzero(votedfor,sizeof(votedfor));
            strncpy(votedfor,&buf[2],IDLEN);
          }
        } 
      }
      delete_old(MAXAGE);
    }
    else if(role==ROLE_LEADER)
    {
      if(buf[0]=='+')          // data (aka heartbeat)
      {
        if(process_line(&buf[2],1)>0)
        {
          // duplicated leaders detected, if we are the weaker, switch roles, otherwise send WTF
          char wtfip[IPLEN+1];
          if(getrank(prevleaderid)<getrank(leaderid))
          {
            strcpy(wtfip,leaderip);
            strcpy(leaderip,ip_self);
            strcpy(leaderid,nodeid);
            sender_add(T_TCP,wtfip,INPUTPORT,WTFMSG);
          }
          else ev_break(EV_A_ EVBREAK_ONE); // switch roles
        }
      }
      // ignore everything else as leader
    }
  }
}


// #aTM0014.1
// 01234567890
// #    - fwd
// a    - proto
// TM00 - sensor name
// 14.1 - value
static void read_tcp_local_cb(struct ev_loop *loop, struct ev_io *w, int revents)
{
  char buf[BUF_SIZE];
  ssize_t len=-1;
  int mlen;
  char *msg,*n;
  char fwdip[IPLEN+1];

  if(!(EV_ERROR&revents))
  {
    len=recv(w->fd,buf,BUF_SIZE,0);
    if(len>0)
    {
      buf[len]='\0';
      printf("%s: %s\n",__func__,buf);
      if(strncmp("quit",buf,4)==0)
      {
        quit=1;
        ev_break(EV_A_ EVBREAK_ONE);
      }
      else if(buf[0]=='#')
      {
        // forward req
        if(strcmp(leaderip,ip_self)==0) strcpy(fwdip,"127.0.0.1");
        else strcpy(fwdip,leaderip);
        printf("leaderip: %s\nip_self: %s\nfwdip: %s\n",leaderip,ip_self,fwdip);
        mlen=strlen(buf)+4+IDLEN+1+1;
        if(NULL!=(msg=malloc(mlen)))
        {
          if(buf[2]==GL_1&&buf[3]==GL_2) n=GLOBALID;
          else n=nodeid;
          snprintf(msg,mlen,"+a 0000%c%c%c%c%s%s",buf[2],buf[3],buf[4],buf[5],n,&buf[6]);
          printf("fwd to %s: '%s'\n",fwdip,msg);
          sender_add(T_TCP,fwdip,INPUTPORT,msg);
          free(msg);
        }
      }
    }
    else if(len<0) fprintf(stderr,"read error\n");
  }
  else fprintf(stderr,"invalid client\n");
  
  if(len==0)
  {
    ev_io_stop(loop,w);
    close(w->fd);
    free(w);
  }
}


static void read_tcp_input_cb(struct ev_loop *loop, struct ev_io *w, int revents)
{
  char buf[BUF_SIZE];
  ssize_t len=-1;

  len=recv(w->fd,buf,BUF_SIZE,0);
  if(role==ROLE_LEADER)
  {
    if(!(EV_ERROR&revents))
    {
      if(len>0)
      {
        buf[len]='\0';
        if(buf[0]=='+') process_line(&buf[2],0);
        else if(strncmp(buf,WTFMSG,sizeof(WTFMSG)-1)==0)
        {
          fprintf(stderr,"WTF --> reader\n");
          ev_break(EV_A_ EVBREAK_ONE);
        }
      }
      else if(len<0) fprintf(stderr,"%s: read error\n",__func__);
    }
    else fprintf(stderr,"%s: invalid client\n",__func__);
  }
  else len=0;
  
  if(len==0)
  {
    ev_io_stop(loop,w);
    close(w->fd);
    free(w);
  }
}


static int bogomips(void)
{
  int ret=0;
  char *p;
  FILE *fp;
  char line[120];

  if(NULL!=(fp=fopen("/proc/cpuinfo","r")))
  {
    while(1)
    {
      if(fgets(line,sizeof(line),fp)==NULL) break;
      for(p=line;*p!='\0';p++) *p=tolower(*p);
      p=strstr(line,"bogomips");
      if(NULL!=p&&NULL!=(p=strchr(p,':'))) { ret=atoi(++p); break; }
    }
    fclose(fp);
  }
  
  return(ret);
}


static int getmachash(void)
{
  int ret=0;
  struct ifaddrs *ifaddr,*ifa;
  struct ifreq ifr;
  int i,n,s;
  unsigned long long macaddress=0;
  char macstr[]="-" XSTR(LONG_LONG_MAX);

  if(-1!=(s=socket(AF_INET,SOCK_DGRAM,IPPROTO_IP))&&getifaddrs(&ifaddr)!=-1)
  {
    for(ifa=ifaddr,n=0;ifa!=NULL;ifa=ifa->ifa_next,n++) 
    {
      if(ifa->ifa_addr==NULL) continue;
      strcpy(ifr.ifr_name, ifa->ifa_name);
      if(ioctl(s,SIOCGIFFLAGS,&ifr)==0) 
      {
        if(!(ifr.ifr_flags&IFF_LOOPBACK))
        { 
          if(ioctl(s,SIOCGIFHWADDR,&ifr)==0) 
          {
            unsigned long long m;
            for(m=i=0;i<6;i++)
            {
              m|=ifr.ifr_hwaddr.sa_data[i]&0xff;
              m<<=8;
            }
            if(m>macaddress) macaddress=m;
          }
        }
      }
    }
    freeifaddrs(ifaddr);
    close(s);
  }
  snprintf(macstr,sizeof(macstr),"%llud",macaddress);
  ret=hash(macstr,72421);

  return(ret);
}


/* TODO:
 *  inotify iface:
 *   - datadir/input <-- notify
 *   - handle files created in that dir as local sensor inputs
 *  demonize
 *  cfg file
 */
int main(void)
{
  int pfdss[2];
  int pfdsf[2];
  int bogo;
  pthread_t pt;
  pthread_t ptf;
  char hostname[32]={0};

  primary_ip(ip_self,sizeof(ip_self));
  bzero(nodeid,sizeof(nodeid));
  bogo=bogomips()/13;
  snprintf(nodeid,sizeof(nodeid),"%02x%02x",bogo>255?255:bogo,(getmachash()&0xff));
  if(gethostname(hostname,sizeof(hostname)-1)==0) strncat(nodeid,hostname,4);
  else strncat("NONE",hostname,4);
  srandom(getmachash()+hash(ip_self,7)+time(NULL));

  if(access(DATADIR,R_OK|W_OK|X_OK)==-1)
  {
    if(0!=mkdir(DATADIR,0770))
    {
      fprintf(stderr,"datadir '%s' missing and unable to create\n",DATADIR);
      exit(1);
    }
  }
  
  if(0!=pipe(pfdss))
  {
    fprintf(stderr,"pipe error 1\n");
    exit(1);
  }
  pipew=pfdss[1];

  if(0!=pipe(pfdsf))
  {
    fprintf(stderr,"pipe error 2\n");
    exit(1);
  }
  pipef=pfdsf[1];

  loop=ev_default_loop(EVBACKEND_SELECT);

  if(0!=(pthread_create(&pt,NULL,sender_thread,(void *)((long)pfdss[0]))))
  {
    fprintf(stderr,"error creating thread for network output!\n");
    exit(1);
  }

  if(0!=(pthread_create(&ptf,NULL,file_thread,(void *)((long)pfdsf[0]))))
  {
    fprintf(stderr,"error creating thread for file io!\n");
    exit(1);
  }

  init_tcp(&tcp_local_sd,loop,&tcp_local_watcher,LOCALPORT,read_tcp_local_cb);  // listen on local tcp input port for local sensor data
  init_udp(&udp_bus_sd,loop,&udp_bus_watcher,BUSPORT,udp_bus_cb);               // listen to broadcast udp bus
  init_udp(&udp_input_sd,loop,&udp_input_watcher,INPUTPORT,udp_input_cb);       // listen udp input port for voting <-- maybe remove and move to bus?
  
  ev_init(&timeout_watcher,timeout_cb);
  
  ev_init(&heartbeat_watcher,heartbeat_cb);
  heartbeat_watcher.repeat=HEARTBEAT;

  quit=0;
  while(quit==0)
  {

    fprintf(stderr,"ROLE_READER\n");
    role=ROLE_READER;
    timeout_watcher.repeat=BUSTMO;//+(random()%15);
    ev_timer_again(loop,&timeout_watcher);
    ev_run(loop,0);
    
    if(quit!=0) break;

    fprintf(stderr,"ROLE_VOTER\n");
    role=ROLE_VOTER;
    timeout_watcher.repeat=VOTETMO;
    ev_timer_again(loop,&timeout_watcher);
    ev_timer_again(loop,&heartbeat_watcher);
    ev_run(loop,0);
    ev_timer_stop(loop,&timeout_watcher);

    if(quit!=0) break;

    if(role!=ROLE_READER)
    {
      fprintf(stderr,"ROLE_LEADER\n");
      set_leader(DATADIR GLDATA);
      role=ROLE_LEADER;
      init_tcp(&tcp_input_sd,loop,&tcp_input_watcher,INPUTPORT,read_tcp_input_cb);  // listen on tcp input port, remote sensor data from peers
      ev_run(loop,0);
      close_tcp(&tcp_input_sd,loop,&tcp_input_watcher);
    }
    ev_timer_stop(loop,&heartbeat_watcher);
  }

  if(write(pipew,"quit",5)<=0) fprintf(stderr,"write error\n");
  pthread_join(pt,NULL);

  if(write(pipef,"quit",5)<=0) fprintf(stderr,"write error\n");
  pthread_join(ptf,NULL);
  
  close_udp(&udp_input_sd,loop,&udp_input_watcher);
  close_tcp(&tcp_input_sd,loop,&tcp_input_watcher);
  close_tcp(&tcp_local_sd,loop,&tcp_local_watcher);
  close_udp(&udp_bus_sd,loop,&udp_bus_watcher);

  return(0);
}