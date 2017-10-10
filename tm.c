// gcc -Wall -fno-strict-aliasing -static -s -Os -o tm tm.c -lev -lm
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>
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
#include <time.h>
#ifdef __linux__
#include <linux/limits.h>
#include <linux/if_link.h>
#include <sys/prctl.h>
#endif
#include <netdb.h>
#include <sys/resource.h>
#ifdef __APPLE__
#include <net/if.h>
#include <net/if_dl.h>
#endif
#include <fts.h>
#include <syslog.h>
#include "coroutine.h"


// CONFIG AREA BEGIN

#ifndef TM_BUFSIZE
#define TM_BUFSIZE 4096               // bufsize (must be > MAXDATA )
#endif
#ifndef TM_DATADIR
#define TM_DATADIR "/var/run/tm_data/"
#endif
#ifndef TM_MAXAGE
#define TM_MAXAGE 3600                // data will be removed after TM_MAXAGE seconds if not refreshed
#endif
#ifndef TM_LOCKFILE
#define TM_LOCKFILE "/var/lock/tm.lock"
#endif
#ifndef TM_DEFAULT_UID
#define TM_DEFAULT_UID 1000
#endif
#ifndef TM_DEFAULT_GID
#define TM_DEFAULT_GID TM_DEFAULT_UID
#endif
#ifndef TM_LOG_IDENT
#define TM_LOG_IDENT "tmd"
#endif
#ifndef TM_MAXERR
#define TM_MAXERR 30                  // max udp send errors
#endif

// CONFIG AREA END



#define STR(s) #s
#define XSTR(s) STR(s)

#define MAXDATA 256                   // maximum length of a data line
#define SENDER_BUFSIZ 512             // sender thread buffer size
#define INPUTDIR TM_DATADIR "in/"
#define TMPDIR TM_DATADIR "tmp/"
#define TMPMASK TMPDIR "tmtmp.XXXXXX"
#define PRI_EV       -8
#define PRI_SENDER   -9
#define PRI_FILE    -12
#define BUSPORT 7697                  // broadcast bus port
#define INPUTPORT 7698                // leader sensor input tcp port and vote input udp port
#define LOCALPORT 7699                // local sensor input fwd-ed to LEADER_IP:INPUTPORT
#define HBCNT_MAX (INT_MAX-1)         // max value of hb count
#define PV 'b'                        // protocol version
#define HEARTBEAT 1000                // 4 leader sequences in every HEARTBEAT ms
#define VOTEHB 150                    // voting heartbeat in ms
#define READERHB  5000                // reader heartbeat in ms
#define BUSTMO ((HEARTBEAT*5)/1000)   // minimum timeout in secs for bus inactivity and start voting session
#define VOTETMO ((VOTEHB*20)/1000)    // voting timeout in secs
#define GL_1 'G'
#define GL_2 'L'
#define GLOBALID "xxxxxxxx"
#define GLDATA "GL00" GLOBALID        // master data name (leader ip and id)
#define HBDATA "HB80"
#define T_UDP 'u'
#define T_TCP 't'
#define ADDR_BROADCAST "255.255.255.255"
#define WTFMSG "WTF"
#define IPLEN (4*3+3)                 // max length of decimal coded ip address
#define IDLEN (8)
#define SNLEN (4)
#define PWRLEN (6)                    // length of "power" string
#define FNAMLEN (IDLEN+SNLEN)
#define WAKEUPLIMIT (60)              // if the last hb was older, wait 2nd cycle
#define FRQ_NORMAL 5                  // normal frames every FRQ_NORMAL th cycles
#define FRQ_RARE   61                 // rare frames every FRQ_RARE th cycle
#define SPORADIC_AGE 60               // sporadic frames until reaches SPORADIC_AGE
#define P_SPORADIC (0xc0)
#define P_RARE     (0x80)
#define P_NORMAL   (0x40)
#define P_URGENT   (0x00)
#define PRIMASK    (0xc0)

#define ROLE_READER 0
#define ROLE_VOTER  1
#define ROLE_LEADER 2

#if HEARTBEAT < VOTEHB
  #error HEARTBEAT must be larger or equal than VOTEHB
#endif

#if READERHB < HEARTBEAT
  #error READERHB must be larger or equal than HEARTBEAT
#endif


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
static char pwr_self[PWRLEN+1];
static char leaderid[IDLEN+1];
static char leaderip[IPLEN+1];
static int leaderpwr=0;
static char prevleaderid[IDLEN+1];
static char prevleaderip[IPLEN+1];
static int prevleaderpwr;
static int quit=0;
static int numhb=0;
static gid_t gid;
static uid_t uid;
static time_t lasthb=0;
static volatile int errcnt_udp=0;
static int udp_input_sd=-1;
static int udp_bus_sd=-1;
static int tcp_local_sd=-1;
static int tcp_input_sd=-1;
static ccrContext frc=0;
static ccrContext src=0;

static ev_io udp_input_watcher;
static ev_io udp_bus_watcher;
static ev_io tcp_local_watcher;
static ev_io tcp_input_watcher;
static ev_signal sigusr1_watcher;
static ev_signal sigusr2_watcher;


// Paul Larson hash
static unsigned int hash(const char* s, unsigned int seed)
{
  unsigned int hash=seed;
    
  while(*s) hash=hash*101+*s++;

  return(hash);
}


static int getpri(const char *nam)
{
  int ret=-1;
  char p[]="00";
  
  if(nam!=NULL)
  {
    p[0]=nam[2];
    p[1]=nam[3];
    ret=strtol(p,NULL,16)&PRIMASK;
  }
  
  return(ret);
}


static int drop_privileges(gid_t gid, uid_t uid)
{
  int ret=0;
  
  if(getuid()==0)
  {
    prctl(PR_SET_KEEPCAPS,1,0,0,0);
    if(setgid(gid)!=0)
    {
      syslog(LOG_ERR,"cannot set gid\n");
      ret=-1;
    }
    if(setuid(uid)!=0)
    {
      syslog(LOG_ERR,"cannot set uid\n");
      ret=-1;
    }
  }
  
  return(ret);
}


static void primary_ip(char *buffer, size_t buflen)
{
  const char *dnsip="8.8.8.8";
  struct sockaddr_in srv;
  int s;
  struct sockaddr_in name;
  socklen_t namelen=sizeof(name);
  
  if(buflen>IPLEN&&buffer!=NULL)
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
          else syslog(LOG_WARNING,"%s: sendto failed %s:%d '%s'\n",__func__,addr,port,msg);
        }
        else syslog(LOG_WARNING,"%s: aton failure %s:%d\n",__func__,addr,port);
      }
      else syslog(LOG_ERR,"%s: cannot enable broadcast\n",__func__);
      close(s);
    }
    else syslog(LOG_CRIT,"%s: cannot create socket\n",__func__);
  }
  else syslog(LOG_ERR,"%s: invalid input\n",__func__);
  if(ret!=0) errcnt_udp++;
  
  return(ret);
}


#define send_tcp(ctx,addr,port,msg,len,s,r,ret) \
do { \
  struct sockaddr_in _si; \
  \
  *ret=-1; \
  if(NULL!=msg&&len>0&&NULL!=addr&&port>0&&port<0x8000) \
  { \
    if((s=socket(PF_INET,SOCK_STREAM|SOCK_NONBLOCK,IPPROTO_TCP))>=0) \
    { \
      fcntl(s, F_SETOWN, getpid()); \
      fcntl(s, F_SETSIG, SIGUSR2); \
      fcntl(s, F_SETFL, fcntl(s,F_GETFL)|O_ASYNC); \
      memset((char *)&_si,0,sizeof(_si)); \
      _si.sin_family=AF_INET; \
      _si.sin_port=htons(port); \
      if(inet_aton(addr,&_si.sin_addr)!=0) \
      { \
        r=connect(s,(struct sockaddr *)&_si,sizeof(_si)); \
        if(r==0||errno==EINPROGRESS||errno==EALREADY) \
        { \
          do { \
            r=send(s,msg,len,0); \
            if(r==len) *ret=0; \
            else if(r==-1) \
            { \
              if(errno==EAGAIN||errno==EWOULDBLOCK) \
              { \
                errno=ctx->en; \
                ccrReturnV; \
                ctx->en=errno; \
              } \
              else r=len; \
            } \
          } while(r!=len); \
          close(s); \
        } \
        else syslog(LOG_WARNING,"%s: cannot connect to %s errno: %d\n",__func__,addr,errno); \
      } \
      else syslog(LOG_WARNING,"%s: aton failure %s:%d\n",__func__,addr,port); \
    } \
    else syslog(LOG_CRIT,"%s: cannot create socket\n",__func__); \
  } \
  else syslog(LOG_ERR,"%s: invalid input\n",__func__); \
  \
} while(0)


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
static void sender_coro(ccrContParam, int f)
{
  ccrBeginContext;
  char buf[SENDER_BUFSIZ+1];
  int fd,s,en,r;
  char *ip,*msg,*n;
  int port,len,l,st;
  ccrEndContext(ctx);

  ccrBegin(ctx);
  ctx->en=0;
  ctx->fd=f;

  fcntl(ctx->fd, F_SETSIG, SIGUSR2);
  fcntl(ctx->fd, F_SETOWN, getpid());
  fcntl(ctx->fd, F_SETFL, fcntl(ctx->fd, F_GETFL)|O_ASYNC);

  while(1)
  {
    ctx->st=1;
    ctx->en=errno;
    ctx->l=read(ctx->fd,ctx->buf,sizeof(ctx->buf));
    if(ctx->l!=-1)
    {
      if(ctx->l>=0) ctx->buf[ctx->l]='\0';
      if(ctx->buf[0]=='q') break;
      // parse addr, port, len
      ctx->ip=&ctx->buf[2];
      if(NULL!=(ctx->n=strchr(ctx->ip,';')))
      {
        *ctx->n='\0';
        ctx->port=atoi(++ctx->n);
        if(NULL!=(ctx->n=strchr(ctx->n,';')))
        {
          ctx->len=atoi(++ctx->n);
          if(NULL!=(ctx->msg=strchr(ctx->n,';')))
          {
            ctx->msg++;
            // TODO: implement the buffer allocation for longer messages
            if(ctx->msg-ctx->buf+ctx->len<=ctx->l)
            {
              if(ctx->buf[0]=='u') ctx->st=send_udp(ctx->ip,ctx->port,ctx->msg,ctx->len);
              else if(ctx->buf[0]=='t') send_tcp(ctx,ctx->ip,ctx->port,ctx->msg,ctx->len,ctx->s,ctx->r,&ctx->st);
              else syslog(LOG_ERR,"unknown command char \"%c\": ",ctx->buf[0]);
            }
            else syslog(LOG_WARNING,"too long message: %ld read only the first %d bytes: ",(long int)(ctx->msg-ctx->buf+ctx->len),ctx->l);
          }
          else syslog(LOG_ERR,"missing message separator: ");
        }
        else syslog(LOG_ERR,"missing length separator: ");
      }
      else syslog(LOG_ERR,"missing ip separator: ");
      if(ctx->st!=0) syslog(LOG_WARNING,"st=%d '%s'\n",ctx->st,ctx->buf);
    }
    else if(errno==EWOULDBLOCK||errno==EAGAIN)
    {
      errno=ctx->en;
      ccrReturnV;
    }
    else syslog(LOG_ERR,"read error: %d\n",errno);
  }

  ccrFinishV;
}


static int write_file(int age, const char *name, const char *str)
{
  int ret=-1;
  int fd,ln;
  char tmpnam[]=TMPMASK;
  struct utimbuf tms;
  
  if(name!=NULL)
  {
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
            if(0==chmod(tmpnam,0644))
            {
              if(0==rename(tmpnam,name)) ret=0;
              else syslog(LOG_WARNING,"%s: rename %s to %s failed\n",__func__,tmpnam,name);
            }
            else syslog(LOG_WARNING,"%s: chmod failed\n",__func__);
          }
          else syslog(LOG_WARNING,"%s: utime failed\n",__func__);
        }
        else syslog(LOG_WARNING,"%s: write failed\n",__func__);
        if(ret!=0) unlink(tmpnam);
      }
      else syslog(LOG_ERR,"%s: mkstemp failed\n",__func__);
    }
    else unlink(name);
  }
  
  return(ret);
}


static int file_create(const char *name, time_t timestamp, const char *data)
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
static void file_coro(ccrContParam, int f)
{
  char *nm,*dt,*n,*buf;
  int len,st;
  time_t ts;
  int q=0;
  ccrBeginContext;
  int fd;
  int en;
  int l;
  char b[SENDER_BUFSIZ+1];
  ccrEndContext(ctx);

  ccrBegin(ctx);
  ctx->fd=f;

  fcntl(ctx->fd, F_SETSIG, SIGUSR1);
  fcntl(ctx->fd, F_SETOWN, getpid());
  fcntl(ctx->fd, F_SETFL, fcntl(ctx->fd, F_GETFL)|O_ASYNC);

  while(q==0)
  {
    ctx->en=errno;
    ctx->l=read(ctx->fd,ctx->b,sizeof(ctx->b));
    if(ctx->l!=-1)
    {
      if(ctx->l>=0) ctx->b[ctx->l]='\0';
      for(st=0,buf=ctx->b;st==0&&buf-ctx->b<ctx->l;)
      {
        st=1;
        if(buf[0]=='q') { q=1; break; }
        // parse timestamp, path and content
        n=&buf[2];
        ts=strtol(n,NULL,10);
        nm=NULL;
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
              if(dt-buf+len<=ctx->l)
              {
                if(buf[0]=='c') st=file_create(nm,ts,dt);
                else if(buf[0]=='d') st=file_delete(nm);
                else syslog(LOG_ERR,"unknown command char \"%c\": ",buf[0]);
                buf=dt+len+1;
              }
              else syslog(LOG_WARNING,"too long data: %ld read only the first %d bytes: ",(long int)(dt-buf+len),ctx->l);
            }
            else syslog(LOG_ERR,"missing data separator: ");
          }
          else syslog(LOG_ERR,"missing length separator: ");
        }
        else syslog(LOG_ERR,"missing timestamp separator: ");
        if(st!=0) syslog(LOG_WARNING,"st=%d '%c' %s",st,buf[0],(nm==NULL?"null":nm));
      }
    }
    else if(errno==EWOULDBLOCK||errno==EAGAIN)
    {
      errno=ctx->en;
      ccrReturnV;
    }
    else syslog(LOG_ERR,"read error: %d\n",errno);
  }
  ccrFinishV;
}


static void sigusr1_cb(struct ev_loop *loop, ev_signal *w, int revents)
{
  file_coro(&frc,0);
}


static void sigusr2_cb(struct ev_loop *loop, ev_signal *w, int revents)
{
  sender_coro(&src,0);
}


// request sending something to somewhere, schedule the request using the pipe which read by the sender thread
static int sender_add(char type, char *addr, int port, char *msg)
{
  int ret=-1;
  char buf[MAXDATA*10];
  char *b=buf;              // u;192.168.1.12;112;7;Message
  char n[]="\n";
  int blen,len,mlen;
  
  if(pipew>0&&addr!=NULL&&port>0&&port<0x8000&&NULL!=msg)
  {
    mlen=strlen(msg);
    blen=strlen(addr)+5+mlen+5+5+2;
    if(blen<sizeof(buf)||NULL!=(b=malloc(blen)))
    {
      if(msg[mlen-1]=='\n') n[0]='\0';
      else n[0]='\n';
      snprintf(b,blen,"%c;%s;%d;%ld;%s%s",type,addr,port,(long int)mlen+(n[0]=='\0'?0:1),msg,n);
      len=strlen(b)+1;
      if(write(pipew,b,len)==len) ret=0;
      if(b!=buf) free(b);
    }
  }
  
  return(ret);
}


// request creating/deleting file, schedule the request using the pipe which read by the file thread
static int file_create_add(int age, const char *dir, const char *name, const char *data)
{
  int ret=-1;
  char buf[250];
  char *b=buf;               	// c;1491024061;/tmp/tm_data/TC00ff33hurk;len;content...
  int blen,len,dlen;
  
  if(pipef>0&&name!=NULL&&NULL!=data)
  {
    dlen=strlen(data);
    blen=strlen(dir)+1+strlen(name)+4+dlen+20+20+2;
    if(blen<sizeof(buf)||NULL!=(b=malloc(blen)))
    {
      snprintf(b,blen,"c;%ld;%s%s;%ld;%s",time(NULL)-age,dir,name,(long int)dlen,data);
      len=strlen(b)+1;
      if(write(pipef,b,len)==len) ret=0;
      //file_coro(&frc,0);
      if(b!=buf) free(b);
    }
  }
  
  return(ret);
}


static int file_delete_add(const char *name)
{
  int ret=-1;
  char buf[250];
  char *b=buf;              // d;0;/tmp/tm_data/TC00ff33hurk;0;
  int blen,len;
  
  if(pipef>0&&name!=NULL)
  {
    blen=strlen(name)+4+3+1;
    if(blen<sizeof(buf)||NULL!=(b=malloc(blen)))
    {
      snprintf(b,blen,"d;0;%s;0;",name);
      len=strlen(b)+1;
      if(write(pipef,b,len)==len) ret=0;
      if(b!=buf) free(b);
    }
  }
  
  return(ret);
}


static void timeout_cb(EV_P_ ev_timer *w, int revents)
{
  time_t now=time(NULL);
  if(difftime(now,lasthb)>WAKEUPLIMIT)
  {
    syslog(LOG_NOTICE,"%s: wakeup limit, wait one more cycle\n",__func__);
    ev_timer_again(loop,&timeout_watcher);
    lasthb=now;
  }
  else ev_break(EV_A_ EVBREAK_ONE);
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
      #ifdef __APPLE
      ret=difftime(now,st.st_mtimespec.tv_sec);
      #endif
      #ifdef __linux__
      ret=difftime(now,st.st_mtim.tv_sec);
      #endif
    }
  }
  
  return(ret);
}


static int getrank(const char *pwr)
{
  int ret=-1;
  char r[]="000000";

  if(pwr!=NULL)
  {
    strncpy(r,pwr,6);
    ret=(int)strtol(r,NULL,16);
  }
  
  return(ret);
}


static void set_leader(char *nam)
{
  char str[PWRLEN+IDLEN+IPLEN+2+1];
  
  if(nam!=NULL)
  {
    // 6634ff,ff6abana,10.0.1.7
    snprintf(str,sizeof(str),"%s,%s,%s",pwr_self,nodeid,ip_self);
    file_create_add(0,TM_DATADIR,nam,str);
  }
  leaderpwr=getrank(pwr_self);
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
      else syslog(LOG_ERR,"%s: bind failed\n",__func__);
    }
    else syslog(LOG_WARNING,"%s: setsockopt REUSEADDR failed\n",__func__);
  }
  else syslog(LOG_ERR,"%s: invalid input\n",__func__);

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


static int delete_old(void)
{
  char nam[sizeof(TM_DATADIR)+FNAMLEN];
  time_t now;
  DIR *d;
  struct dirent *e;
  int a;

  now=time(NULL);
  if(NULL!=(d=opendir(TM_DATADIR)))
  {
    while((e=readdir(d)))
    {
      if((DT_REG==e->d_type||DT_UNKNOWN==e->d_type)&&e->d_name[0]!='.'&&strlen(e->d_name)<=FNAMLEN)
      {
        strcpy(nam,TM_DATADIR);
        strcat(nam,e->d_name);
        a=getfileage(nam,now);
        if(a>TM_MAXAGE||(getpri(e->d_name)==P_SPORADIC&&a>SPORADIC_AGE)) file_delete_add(nam);
      }
    }
    closedir(d);
  }

  return(0);
}


static void updatepwr(char *p)
{
  char v[]="00";
  snprintf(v,sizeof(v),"%02x",(numhb/3600>0xff?0xff:numhb/3600));
  if(p) { p[0]=v[0]; p[1]=v[1]; }
}


static void heartbeat_cb(EV_P_ ev_timer *w, int revents)
{
  static time_t lasthb=0;
  char pdu[TM_BUFSIZE];
  char nam[sizeof(TM_DATADIR)+FNAMLEN];
  char age[]="0000";
  char *p;
  int c,len,a;
  time_t now;
  
  now=time(NULL);
  if(difftime(lasthb,now)*1000<READERHB*5)
  {
    pdu[0]='\0';
    if(pipew>0)
    {
      if(role==ROLE_VOTER)
      {
        // send voting request "?b<6PWR>,<8NODEID>,<?IP>"
        snprintf(pdu,sizeof(pdu),"?%c%s,%s,%s",PV,pwr_self,nodeid,ip_self);
        if(pdu[0]!='\0') sender_add(T_UDP,ADDR_BROADCAST,BUSPORT,pdu);
      }
      else if(role==ROLE_LEADER)
      {
        // send data to bus
        FILE *fp;
        DIR *d;
        struct dirent *e;
        int ispnormal,isprare,pri;
        
        if(++numhb>=HBCNT_MAX) numhb=HBCNT_MAX;
        if(errcnt_udp<TM_MAXERR)
        {
          ispnormal=numhb%FRQ_NORMAL;
          isprare=numhb%FRQ_RARE;
          updatepwr(pwr_self);
          set_leader(GLDATA);
          pdu[0]='+';
          pdu[1]=PV;
          pdu[2]='\0';
          p=&pdu[2];
          len=sizeof(pdu)-3;
          if(NULL!=(d=opendir(TM_DATADIR)))
          {
            while((e=readdir(d)))
            {
              if((DT_REG==e->d_type||DT_UNKNOWN==e->d_type)&&e->d_name[0]!='.'&&strlen(e->d_name)<=FNAMLEN)
              {
                strcpy(nam,TM_DATADIR);
                strcat(nam,e->d_name);
                if(len<IDLEN+4+1)
                {
                  syslog(LOG_WARNING,"%s: buffer too small, skipping '%s'\n",__func__,e->d_name);
                  continue;
                }
                pri=getpri(e->d_name);
                if(pri==P_NORMAL&&ispnormal!=0) continue;
                if(pri==P_RARE&&isprare!=0) continue;
                a=getfileage(nam,now);
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
                    syslog(LOG_WARNING,"%s: read error '%s' skipped\n",__func__,e->d_name);
                    p-=IDLEN+4+4+1;
                    len+=IDLEN+4+4+1;
                  }
                  fclose(fp);
                }
                else syslog(LOG_WARNING,"%s: file open error '%s'\n",__func__,e->d_name);
              }
            }
            closedir(d);
          }
          delete_old();
          if(pdu[0]!='\0') sender_add(T_UDP,ADDR_BROADCAST,BUSPORT,pdu);
        }
        else
        {
          syslog(LOG_ERR,"%s: err cnt > %d drop leadership\n",__func__,TM_MAXERR);
          ev_break(EV_A_ EVBREAK_ONE);
        }
      }
      else if(role==ROLE_READER)
      {
        snprintf(pdu,sizeof(pdu),"#%c" HBDATA "%s",PV,ip_self);
        sender_add(T_TCP,"127.0.0.1",LOCALPORT,pdu);
      }
    }
  }
  else numhb=0;
  lasthb=now;
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
          else syslog(LOG_WARNING,"%s: accept failed\n",__func__);
        }
        else syslog(LOG_WARNING,"%s: invalid event\n",__func__);
      }
      else syslog(LOG_WARNING,"%s: missing tcp_data field\n",__func__);
    }
    else syslog(LOG_WARNING,"%s: invalid watcher\n",__func__);
    if(st!=0) free(wc);
  }
  else syslog(LOG_CRIT,"%s: no memory\n",__func__);
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
            else syslog(LOG_WARNING,"%s: listen failed\n",__func__);
          }
          else syslog(LOG_WARNING,"%s: cannot bind socket\n",__func__);
        }
        else syslog(LOG_WARNING,"%s: cannot set SO_REUSEADDR on socket\n",__func__);
      }
      else syslog(LOG_CRIT,"%s: cannot create socket\n",__func__);
    }
    else syslog(LOG_CRIT,"%s: no memory\n",__func__);
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
    if(!dry) file_create_add(age,TM_DATADIR,n,d);
    ret=0;
    if(strcmp(n,GLDATA)==0)
    {
      char *lid,*ip;
      // pppppp,nnnnnnnn,<ip>
      // 01234567890123456
      if(NULL!=(lid=strchr(d,',')))
      {
        *lid++='\0';
        if(NULL!=(ip=strchr(lid,',')))
        {
          *ip++='\0';
          if(strlen(ip)<=IPLEN&&strcmp(leaderip,ip)!=0&&strlen(lid)<=IDLEN)
          {
            ret=1;
            prevleaderpwr=leaderpwr;
            leaderpwr=getrank(d);
            strcpy(prevleaderip,leaderip);
            strcpy(prevleaderid,leaderid);
            strcpy(leaderip,ip);
            strcpy(leaderid,lid);
          }
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


static void udp_input_cb(struct ev_loop *loop, ev_io *w, int revents)
{
  char buf[TM_BUFSIZE];
  struct sockaddr_in addr;
  int addr_len=sizeof(addr);
  socklen_t len;
  
  len=recvfrom(w->fd,buf,sizeof(buf)-1,0,(struct sockaddr*)&addr,(socklen_t *)&addr_len);
  if(role==ROLE_VOTER)
  {
    if(len>0)
    {
      buf[len]='\0';
      if(buf[0]=='!'&&buf[1]==PV)
      {
        int ps,pr;
        ps=getrank(pwr_self);
        pr=getrank(&buf[2]);
        if(ps<pr)
        {
          syslog(LOG_NOTICE,"rank self %d < rank %.8s=%d, voting lost\n",ps,&buf[2+PWRLEN+1],pr);
          role=ROLE_READER;
          ev_break(EV_A_ EVBREAK_ONE);
        }
      }
    }
  }
}


static void udp_bus_cb(struct ev_loop *loop, ev_io *w, int revents)
{
  static time_t lastvoted=0;
  static char votedfor[IDLEN+1]={0};
  static int votedforpwr=0;
  time_t now;
  char buf[TM_BUFSIZE];
  struct sockaddr_in addr;
  int addr_len=sizeof(addr);
  socklen_t len;

  len=recvfrom(w->fd,buf,sizeof(buf)-1,0,(struct sockaddr*)&addr,(socklen_t *)&addr_len);
  if(len>0&&buf[1]==PV)
  {
    buf[len-1]='\0';
    if(role==ROLE_READER||role==ROLE_VOTER)
    {
      if(role==ROLE_READER) ev_timer_again(loop,&timeout_watcher);
      now=time(NULL);
      if(++numhb>=HBCNT_MAX) numhb=HBCNT_MAX;
      if(buf[0]=='+')          // data (aka heartbeat)
      {
        if(process_line(&buf[2],0)>0)
        {
          // leaderip changed, check conditions
          if(difftime(now,lasthb)<BUSTMO)
          {
            // double leaders, send WTF to the weaker
            char wtfip[IPLEN+1];
            if(prevleaderpwr>leaderpwr) strcpy(wtfip,prevleaderip);
            else strcpy(wtfip,leaderip);
            sender_add(T_TCP,wtfip,INPUTPORT,WTFMSG);
          }
        }
        lasthb=now;
      }
      else if(buf[0]=='?')
      {
        // voting request "?b<6PWR>,<8NODEID>,<?IP>"
        //                 ?bpppppp,NNNNNNNN,<IP>
        //                           11111
        //                 012345678901234...
        int voteage=difftime(now,lastvoted);
        if(       voteage>VOTETMO
            || (  voteage<VOTETMO && strncmp(votedfor,&buf[9],sizeof(votedfor))==0 )
            || ( (voteage<VOTETMO || lastvoted==0) && getrank(&buf[2])>votedforpwr )
          )
        {
          int votelen;
          char *vote;
          votelen=strlen(buf)+IDLEN+2;
          if(NULL!=(vote=malloc(votelen)))
          {
            snprintf(vote,votelen,"!%s,%s",&buf[1],nodeid);
            sender_add(T_UDP,ADDR_BROADCAST,INPUTPORT,vote);
            free(vote);
            lastvoted=now;
            bzero(votedfor,sizeof(votedfor));
            strncpy(votedfor,&buf[2],IDLEN);
            votedforpwr=getrank(&buf[2]);
          }
        }
      }
      delete_old();
    }
    else if(role==ROLE_LEADER)
    {
      if(buf[0]=='+')          // data (aka heartbeat)
      {
        if(process_line(&buf[2],1)>0)
        {
          // duplicated leaders detected, if we are the weaker, switch roles, otherwise send WTF
          char wtfip[IPLEN+1];
          if(prevleaderpwr<leaderpwr)
          {
            syslog(LOG_NOTICE,"rank %d > other leader %s=%d, sending WTF\n",leaderpwr,leaderid,prevleaderpwr);
            strcpy(wtfip,leaderip);
            strcpy(leaderip,ip_self);
            strcpy(leaderid,nodeid);
            sender_add(T_TCP,wtfip,INPUTPORT,WTFMSG);
          }
          else
          {
            syslog(LOG_NOTICE,"rank %d < new leader %s=%d, switch to reader\n",leaderpwr,leaderid,prevleaderpwr);
            ev_break(EV_A_ EVBREAK_ONE); // switch roles
          }
        }
      }
      // ignore everything else as leader
    }
  }
}


static int forward_sensor_input(const char *nam, const char *buf)
{
  int ret=-1;
  char fwdip[IPLEN+1];
  int mlen;
  char *msg;
  const char *n;
  
  if(NULL!=buf&&NULL!=nam)
  {
    if(strcmp(leaderip,ip_self)==0) strcpy(fwdip,"127.0.0.1");
    else strcpy(fwdip,leaderip);
    mlen=strlen(buf)+strlen(nam)+3+4+IDLEN+1+1;
    if(NULL!=(msg=malloc(mlen)))
    {
      if(buf[2]==GL_1&&buf[3]==GL_2) n=GLOBALID;
      else n=nodeid;
      snprintf(msg,mlen,"+%c 0000%s%s%s",PV,nam,n,buf);
      ret=sender_add(T_TCP,fwdip,INPUTPORT,msg);
      free(msg);
    }
  }
    
  return(ret);
}


// #aTM0014.1
// 01234567890
// #    - fwd
// a    - proto
// TM00 - sensor name
// 14.1 - value
static void read_tcp_local_cb(struct ev_loop *loop, struct ev_io *w, int revents)
{
  char buf[TM_BUFSIZE];
  char sn[SNLEN+1]={0};
  ssize_t len=-1;

  if(!(EV_ERROR&revents))
  {
    len=recv(w->fd,buf,sizeof(buf),0);
    if(len>0)
    {
      buf[len]='\0';
      if(strncmp("quit",buf,4)==0)
      {
        quit=1;
        syslog(LOG_NOTICE,"local quit request\n");
        ev_break(EV_A_ EVBREAK_ONE);
      }
      else if(buf[0]=='#'&&buf[1]==PV)
      {
        strncpy(sn,&buf[2],SNLEN);
        forward_sensor_input(sn,&buf[6]);
      }
    }
    else if(len<0) syslog(LOG_WARNING,"read error\n");
  }
  else syslog(LOG_WARNING,"invalid client\n");
  
  if(len==0)
  {
    ev_io_stop(loop,w);
    close(w->fd);
    free(w);
  }
}


static void input_dir_cb(struct ev_loop *loop, struct ev_stat *w, int revents)
{
  DIR *d;
  struct dirent *e;
  char nam[sizeof(INPUTDIR)+SNLEN];
  char buf[TM_BUFSIZE];
  int len;
  FILE *f;

  if(!(EV_ERROR&revents)&&w->attr.st_nlink)
  {
    // scan dir for files, process and delete them
    if(NULL!=(d=opendir(INPUTDIR)))
    {
      while((e=readdir(d)))
      {
        if((DT_REG==e->d_type||DT_UNKNOWN==e->d_type)&&e->d_name[0]!='.'&&strlen(e->d_name)<=SNLEN)
        {
          strcpy(nam,INPUTDIR);
          strcat(nam,e->d_name);
          if(NULL!=(f=fopen(nam,"rb")))
          {
            len=fread(buf,1,sizeof(buf),f);
            fclose(f);
            if(len>0)
            {
              buf[len]='\0';
              if(0==forward_sensor_input(e->d_name,buf)) file_delete_add(nam);
            }
          }
        }
      }
      closedir(d);
    }
  }
}


static void read_tcp_input_cb(struct ev_loop *loop, struct ev_io *w, int revents)
{
  char buf[TM_BUFSIZE];
  ssize_t len=-1;

  len=recv(w->fd,buf,sizeof(buf),0);
  if(role==ROLE_LEADER)
  {
    if(!(EV_ERROR&revents))
    {
      if(len>0)
      {
        buf[len]='\0';
        if(buf[0]=='+'&&buf[1]==PV) process_line(&buf[2],0);
        else if(strncmp(buf,WTFMSG,sizeof(WTFMSG)-1)==0)
        {
          syslog(LOG_NOTICE,"WTF received, switch to reader\n");
          ev_break(EV_A_ EVBREAK_ONE);
        }
      }
      else if(len<0) syslog(LOG_WARNING,"%s: read error\n",__func__);
    }
    else syslog(LOG_WARNING,"%s: invalid client\n",__func__);
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


#ifdef __linux__
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
#endif


#ifdef __APPLE__
// http://stackoverflow.com/questions/3964494/having-a-problem-figuring-out-how-to-get-ethernet-interface-info-on-mac-os-x-usi#4267204
static int getmachash(void)
{
  int ret=0;
  struct ifaddrs *if_addrs = NULL;
  struct ifaddrs *if_addr = NULL;
  void *tmp = NULL;
  unsigned long long macaddress=0,m;
  char macstr[]="-" XSTR(LONG_LONG_MAX);
  int i;

  if (0 == getifaddrs(&if_addrs)) {
    for (if_addr = if_addrs; if_addr != NULL; if_addr = if_addr->ifa_next) {
      // Address
      if (if_addr->ifa_addr->sa_family == AF_INET) {
        tmp = &((struct sockaddr_in *)if_addr->ifa_addr)->sin_addr;
      } else {
        tmp = &((struct sockaddr_in6 *)if_addr->ifa_addr)->sin6_addr;
      }
      // Mask
      if (if_addr->ifa_netmask != NULL) {
        if (if_addr->ifa_netmask->sa_family == AF_INET) {
          tmp = &((struct sockaddr_in *)if_addr->ifa_netmask)->sin_addr;
        } else {
          tmp = &((struct sockaddr_in6 *)if_addr->ifa_netmask)->sin6_addr;
        }
      }
      // MAC address
      if (if_addr->ifa_addr != NULL && if_addr->ifa_addr->sa_family == AF_LINK) {
        struct sockaddr_dl* sdl = (struct sockaddr_dl *)if_addr->ifa_addr;
        unsigned char mac[6];
        if (6 == sdl->sdl_alen) {
          memcpy(mac, LLADDR(sdl), sdl->sdl_alen);
          for(m=i=0;i<6;i++)
          {
            m|=mac[i]&0xff;
            m<<=8;
          }
          if(m>macaddress) macaddress=m;
        }
      }
    }
    freeifaddrs(if_addrs);
    if_addrs = NULL;
  }
  snprintf(macstr,sizeof(macstr),"%llud",macaddress);
  ret=hash(macstr,72421);

  return(ret);
}
#endif


// http://stackoverflow.com/a/27808574
static int recursive_delete(const char *dir)
{
  int ret=0;
  FTS *ftsp=NULL;
  FTSENT *curr;
  char *files[]={ (char *) dir, NULL };

  ftsp=fts_open(files, FTS_NOCHDIR | FTS_PHYSICAL | FTS_XDEV, NULL);
  if(!ftsp) 
  {
    ret=-1;
    goto finish;
  }

  while((curr=fts_read(ftsp)))
  {
    switch(curr->fts_info)
    {
      case FTS_DP:
      case FTS_F:
      case FTS_SL:
      case FTS_SLNONE:
      case FTS_DEFAULT:
        if(remove(curr->fts_accpath)<0) ret=-1;
        break;
    }
  }

finish:
    if(ftsp) fts_close(ftsp);

    return(ret);
}


static void daemonize(void)     // from http://www.enderunix.org/documents/eng/daemon.php
{
  int i,lfp;
  char str[]="-" XSTR(LONG_MAX);

  if(1==getppid()) return;                              // already a daemon
  i=fork();
  if(i<0) exit(1);                                      // fork error
  if(i>0) exit(0);                                      // parent exits
  // child (daemon) continues
  setsid();                                             // obtain a new process group
  for(i=getdtablesize();i>=0;--i) close(i);             // close all descriptors
  i=open("/dev/null",O_RDWR);
  if(-1==dup(i)) exit(1);
  if(-1==dup(i)) exit(1);
  umask(027);                                           // set newly created file permissions
  lfp=open(TM_LOCKFILE,O_RDWR|O_CREAT,0640);
  if(lfp<0) exit(1);                                    // can not open
  if(lockf(lfp,F_TLOCK,0)<0) exit(0);                   // can not lock
  // first instance continues
  snprintf(str,sizeof(str),"%d\n",getpid());
  if(-1==write(lfp,str,strlen(str))) exit(1);
  signal(SIGCHLD,SIG_IGN);                              // ignore child
  signal(SIGTSTP,SIG_IGN);                              // ignore tty signals
  signal(SIGTTOU,SIG_IGN);
  signal(SIGTTIN,SIG_IGN);
}


/* TODO:
 *  cfg file (datadir,logging)
 *  input_dir_cb --> move dir scan to file io thread (forward_* needs a variation with local send)
 *  implement priority for sensors: (id 2 MSB)
 *            - 00xx xxxx 0x important: every cycle (1sec in b)
 *            - 01xx xxxx 4x normal: every 5th cycle
 *            - 10xx xxxx 8x rare: every 61th cycle
 *            - 11xx xxxx cx sporadic: every cycle until age < 120secs
 * DONE:
 *  logging
 *  heartbeat for readers
 *  put TM_DATADIR to /run
 *  switch to voter mode if vote related traffic detected in reader mode
 *  remove old files --> move to file io thread
 *  nodeid: change to hash+hostname only (hash= add all mac addresses as 64bit int and get 101 hash)
 *  power value: 24bit value 6chars: daemon uptime/8h, speed related byte, nodeid first byte
 *               this should goes to GL00 sensor data and used during voting
 *  separate vote hb: vote hb now is the normal heartbeat (1sec) this should be separated and should
 *                    be shorter (~1-200ms)
 *  after vote hb: reduce votetmo
 *
 */
int main(int argc, char **argv)
{
  int pfdss[2]={0};
  int pfdsf[2]={0};
  int bogo=0;
  char hostname[32]={0};
  ev_stat finput;
  int o,dmn=0,machash;
  mode_t m;

  uid=TM_DEFAULT_UID;
  gid=TM_DEFAULT_GID;
  while((o=getopt(argc,argv,"dhu:g:"))!=-1)
  {
    switch(o)
    {
      case 'u':
        uid=atoi(optarg);
        break;
      case 'g':
        gid=atoi(optarg);
        break;
      case 'd':
        dmn=1;
        break;
      case 'h':
      default:
        fprintf(stderr,"semi-realtime telemetry\n(c) Gergely Gati 2017 AGPL\nusage: %s [-d] [-u uid] [-g gid]\n",argv[0]);
        exit(0);
    }
  }

  if(getuid()!=0)
  {
    fprintf(stderr,"run as root\n");
    exit(1);
  }

  if(dmn!=0) daemonize();

  openlog(TM_LOG_IDENT,LOG_PID|LOG_NOWAIT,LOG_USER);
  recursive_delete(TM_DATADIR);
  primary_ip(ip_self,sizeof(ip_self));
  bzero(nodeid,sizeof(nodeid));
  bogo=bogomips()/13;
  machash=getmachash();
  snprintf(pwr_self,sizeof(pwr_self),"00%02x%02x",bogo>0xff?0xff:bogo,machash&0xff);
  snprintf(nodeid,sizeof(nodeid),"%04x",(machash&0xffff));
  if(gethostname(hostname,sizeof(hostname)-1)==0) strncat(nodeid,hostname,4);
  else strncat("NONE",hostname,4);
  srandom(getmachash()+hash(ip_self,7)+time(NULL));

  m=umask(0);
  if(access(TM_DATADIR,R_OK|W_OK|X_OK)==-1)
  {
    if(0!=mkdir(TM_DATADIR,0755))
    {
      fprintf(stderr,"datadir '%s' missing and unable to create\n",TM_DATADIR);
      exit(1);
    }
  }
  if(0!=chown(TM_DATADIR,uid,gid))
  {
    fprintf(stderr,"chown %d:%d %s failed\n",uid,gid,TM_DATADIR);
    exit(1);
  }
  if(access(INPUTDIR,R_OK|W_OK|X_OK)==-1)
  {
    if(0!=mkdir(INPUTDIR,0733))
    {
      fprintf(stderr,"inputdir '%s' missing and unable to create\n",INPUTDIR);
      exit(1);
    }
  }
  if(0!=chown(INPUTDIR,uid,gid))
  {
    fprintf(stderr,"chown %d:%d %s failed\n",uid,gid,INPUTDIR);
    exit(1);
  }
  if(access(TMPDIR,R_OK|W_OK|X_OK)==-1)
  {
    if(0!=mkdir(TMPDIR,0700))
    {
      fprintf(stderr,"tmpdir '%s' missing and unable to create\n",TMPDIR);
      exit(1);
    }
  }
  if(0!=chown(TMPDIR,uid,gid))
  {
    fprintf(stderr,"chown %d:%d %s failed\n",uid,gid,TMPDIR);
    exit(1);
  }

  if(0!=pipe2(pfdss,O_NONBLOCK))
  {
    fprintf(stderr,"pipe error 1\n");
    exit(1);
  }
  pipew=pfdss[1];

  if(0!=pipe2(pfdsf,O_NONBLOCK))
  {
    fprintf(stderr,"pipe error 2\n");
    exit(1);
  }
  pipef=pfdsf[1];
  
  loop=ev_default_loop(EVBACKEND_SELECT);

  ev_signal_init(&sigusr2_watcher, sigusr2_cb, SIGUSR2);
  ev_signal_start(loop, &sigusr2_watcher);
  sender_coro(&src,pfdss[0]);

  ev_signal_init(&sigusr1_watcher, sigusr1_cb, SIGUSR1);
  ev_signal_start(loop, &sigusr1_watcher);
  file_coro(&frc,pfdsf[0]);

  setpriority(PRIO_PROCESS,0,PRI_EV);
  drop_privileges(gid,uid);

  init_tcp(&tcp_local_sd,loop,&tcp_local_watcher,LOCALPORT,read_tcp_local_cb);  // listen on local tcp input port for local sensor data
  init_udp(&udp_bus_sd,loop,&udp_bus_watcher,BUSPORT,udp_bus_cb);               // listen to broadcast udp bus
  init_udp(&udp_input_sd,loop,&udp_input_watcher,INPUTPORT,udp_input_cb);       // listen udp input port for voting <-- maybe remove and move to bus?
  
  ev_stat_init(&finput,input_dir_cb,INPUTDIR,0.);
  ev_stat_start(loop,&finput);

  ev_init(&timeout_watcher,timeout_cb);
  
  ev_init(&heartbeat_watcher,heartbeat_cb);
  heartbeat_watcher.repeat=HEARTBEAT/1000.0;

  quit=0;
  while(quit==0)
  {
    syslog(LOG_NOTICE,"ROLE_READER\n");
    numhb=0; updatepwr(pwr_self); // reset age counter
    heartbeat_watcher.repeat=READERHB/1000.0;
    ev_timer_again(loop,&heartbeat_watcher);
    role=ROLE_READER;
    timeout_watcher.repeat=BUSTMO;
    ev_timer_again(loop,&timeout_watcher);
    ev_run(loop,0);
    
    if(quit!=0) break;

    syslog(LOG_NOTICE,"ROLE_VOTER\n");
    updatepwr(pwr_self);
    errcnt_udp=0;
    role=ROLE_VOTER;
    usleep((random()%VOTEHB)*1000);
    timeout_watcher.repeat=VOTETMO;
    ev_timer_again(loop,&timeout_watcher);
    heartbeat_watcher.repeat=VOTEHB/1000.0;
    ev_timer_again(loop,&heartbeat_watcher);
    ev_run(loop,0);
    ev_timer_stop(loop,&timeout_watcher);
    heartbeat_watcher.repeat=HEARTBEAT/1000.0;
    ev_timer_again(loop,&heartbeat_watcher);

    if(quit!=0) break;

    if(role!=ROLE_READER&&errcnt_udp==0)
    {
      syslog(LOG_NOTICE,"ROLE_LEADER\n");
      errcnt_udp=0;
      set_leader(GLDATA);
      role=ROLE_LEADER;
      init_tcp(&tcp_input_sd,loop,&tcp_input_watcher,INPUTPORT,read_tcp_input_cb);  // listen on tcp input port, remote sensor data from peers
      ev_run(loop,0);
      close_tcp(&tcp_input_sd,loop,&tcp_input_watcher);
    }
  }

  if(write(pipew,"quit",5)<=0) syslog(LOG_WARNING,"write error\n");
  sender_coro(&src,0);
  ccrAbort(src);

  if(write(pipef,"quit",5)<=0) syslog(LOG_WARNING,"write error\n");
  file_coro(&frc,0);
  ccrAbort(frc);
  
  close_udp(&udp_input_sd,loop,&udp_input_watcher);
  close_tcp(&tcp_input_sd,loop,&tcp_input_watcher);
  close_tcp(&tcp_local_sd,loop,&tcp_local_watcher);
  close_udp(&udp_bus_sd,loop,&udp_bus_watcher);

  closelog();
  umask(m);

  return(0);
}
