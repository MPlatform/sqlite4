/*
** This utility program reads the header (the "checkpoint" information) from
** an LSM SQLite4 database file and displays it as human-readable text.
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void usage(const char *argv){
  fprintf(stderr, "Usage: %s DBFILE 1|2\n", argv);
  exit(1);
}

static unsigned int getInt(const unsigned char *a, int n){
  a += n*4;
  return (a[0]<<24) | (a[1]<<16) | (a[2]<<8) | a[3];
}

static void prline(const char *zTitle, unsigned v){
  int n = (int)strlen(zTitle);
  printf("%s%.*s %10u %08x\n", zTitle,
    55-n, "............................................................",
    v, v);
}

static void prsegment(const char *zName, unsigned char *aData){
  char zTitle[100];
  snprintf(zTitle, sizeof(zTitle), "%s first page", zName);
  prline(zTitle, getInt(aData, 0));
  snprintf(zTitle, sizeof(zTitle), "%s last page", zName);
  prline(zTitle, getInt(aData, 1));
  snprintf(zTitle, sizeof(zTitle), "%s root page", zName);
  prline(zTitle, getInt(aData, 2));
  snprintf(zTitle, sizeof(zTitle), "%s number of pages", zName);
  prline(zTitle, getInt(aData, 3));
}

int main(int argc, char **argv){
  FILE *in;
  int pgno;
  unsigned sz, nLevel, nRight, nMerge;
  unsigned iLevel, iRight, iMerge;
  unsigned base;
  unsigned nFree;
  unsigned ovflFlag;
  char z[100];
  unsigned char aPage[4200];

  if( argc!=3 ) usage(argv[0]);
  pgno = atoi(argv[2]);
  if( pgno!=1 && pgno!=2 ) usage(argv[0]);
  in = fopen(argv[1], "rb");
  if( in==0 ) usage(argv[0]);
  if( pgno==2 ) fseek(in, 4096, SEEK_SET);
  fread(aPage, 1, 4096, in);
  memset(aPage+4096, 0, 100);
  prline("Checkpoint id MSW", getInt(aPage,0));
  prline("Checkpoint id LSW", getInt(aPage,1));
  sz = getInt(aPage,2);
  prline("Values in checkpoint", sz);
  prline("Blocks in database", getInt(aPage,3));
  prline("Block size", getInt(aPage,4));
  nLevel = getInt(aPage,5);
  prline("Number of levels", nLevel);
  prline("Database page size", getInt(aPage,6));
  ovflFlag = getInt(aPage, 7);
  prline("LEVELS and FREELIST records in L0?", ovflFlag);
  base = 8;

  prline("Log pointer #1", getInt(aPage,base));
  prline("Log pointer #2", getInt(aPage,base+1));
  prline("Log pointer #3", getInt(aPage,base+2));
  prline("Log pointer #4", getInt(aPage,base+3));

  base += 4;
  for(iLevel=0; iLevel<nLevel && base<1024; iLevel++){
    printf("Level[%d]:\n", iLevel);
    prline("  Age of this level", getInt(aPage, base));
    nRight = getInt(aPage, base+1);
    prline("  Number of right-hand segments", nRight);
    base += 2;
    prsegment("  Left segment", aPage + (base*4));
    base += 4;
    for(iRight=0; iRight<nRight && base<1024; iRight++){
      snprintf(z, 50, "  Right segment %d", iRight);
      prsegment(z, aPage + base*4);
      base += 4;
    }
    if( nRight>0 ){
      nMerge = getInt(aPage, base); base++;
      prline("  Number of segments involved in the merge", nMerge);
      prline("  Current nSkip value", getInt(aPage, base)); base++;
      for(iMerge=0; iMerge<nMerge && base<1024; iMerge++){
        snprintf(z, 80, "  Right segment %d next page", iMerge);
        prline(z, getInt(aPage, base));
        snprintf(z, 80, "  Right segment %d next cell", iMerge);
        prline(z, getInt(aPage, base+1));
        base += 2;
      }
      prline("  Split-key page", getInt(aPage, base));
      prline("  Split-key cell", getInt(aPage, base));
      base += 2;
    }
  }
  if( base>=1020 ) return 0;

  if( ovflFlag==0 ){
    unsigned nFree = getInt(aPage, base);
    unsigned i;
    prline("Number of on-checkpoint freelist blocks", nFree);
    for(i=0; i<nFree; i++){
      snprintf(z, 80, "Freelist block %d", i);
      prline(z, getInt(aPage, base+i+1));
    }
    base += nFree+1;
  }else{
    prline("Size to truncate free list to after loading", getInt(aPage, base));
    prline("First refree block", getInt(aPage, base+1));
    prline("Second refree block", getInt(aPage, base+2));
    base += 3;
  }
  prline("Checksum value 1", getInt(aPage, base));
  prline("Checksum value 2", getInt(aPage, base+1));

  base += 2;
  printf("****************************************"
         "***************************************\n");
  printf("Used %d out 1024 integers available in the header (%d%%)\n",
    base, base*100/1024);
   
}
