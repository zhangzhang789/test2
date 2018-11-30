#!usr/bin/perl -w
use Time::Local;  
use strict;
use Net::FTP;




my $home = $ENV{'PWD'};
#print "pwd=$home\n";

#ע���޸����ݿ��ַ���û���������
my ($dbname,$usr,$psw) = ('10.254.4.2','bmnc_dba','bmnc_dba123');    #��дȡ�����ݿ�ĵ�ַ���û����Ϳ���
#my $tx_date = $ARGV[1];
my $tx_date = $ARGV[0];

my $tmpfile = "t98_pasgr_date_st.tmp.dat";       #�ϴ��н飬�ϴ���ftp��Ҫɾ��
my $outfile = "t98_pasgr_date_st${tx_date}.csv"; #������ݺ󱣴��csv�������dat��Ȼ����ɾ��



print "��ʼ������ $tx_date\n";


my $rc = open(FEXP,"| fexp");
   unless ($rc){
      print "Could not invoke FEXP command\n";
      return 1;                              
   }

print FEXP <<ENDOFINPUT;

.LOGTABLE bmnc_temp.t_t98_pasgr_date_st_exp2;
.logon $dbname/$usr,$psw;
.BEGIN EXPORT SESSIONS 20;
.EXPORT OUTFILE /home/etl/DATA/process/$tmpfile MODE RECORD FORMAT TEXT;

select cast(   #�޸�Ҫ��ѯ�ı�ṹ
	trim(cast(Stat_Dt as VARCHAR(20)))||cast(',' as char(1))||
	trim(cast(Stat_Period_Cd as CHAR(2)))||cast(',' as char(1))||
	trim(cast(Prod_ID as VARCHAR(60)))||cast(',' as char(1))||
	trim(cast(Gate_ID as VARCHAR(100)))||cast(',' as char(1))||
	trim(cast(Station_Hall_ID as VARCHAR(60)))||cast(',' as char(1))||
	trim(cast(Station_ID as VARCHAR(30)))||cast(',' as char(1))||
	trim(cast(Line_ID as VARCHAR(30)))||cast(',' as char(1))||
	trim(cast(Trip_Drct_Cd as CHAR(2)))||cast(',' as char(1))||
	trim(cast(Data_Stat_Std_Cd as CHAR(2)))||cast(',' as char(1))||
	trim(cast(Data_Dt as VARCHAR(20)))||cast(',' as char(1))||
	trim(cast(cast(Entry_Quatity as number(18)) as VARCHAR(20))) ||cast(',' as char(1))||
	trim(cast(cast(Exit_Quatity as number(18)) as VARCHAR(20))) ||cast(',' as char(1))||
	trim(cast(cast(Swipe_Card_Entry_Quatity as number(18)) as VARCHAR(20))) ||cast(',' as char(1))||
	trim(cast(cast(Swipe_Card_Exit_Quatity as number(18)) as VARCHAR(20))) as char(500)		
)
from		BMNC_Pmart.t98_pasgr_date_st
where	data_dt = cast('${tx_date}' as date format 'yyyymmdd') 
and Data_Stat_Std_Cd='01'

union all 

select cast(
	trim(cast(Stat_Dt as VARCHAR(20)))||cast(',' as char(1))||
	trim(cast(Stat_Period_Cd as CHAR(2)))||cast(',' as char(1))||
	trim(cast(Prod_ID as VARCHAR(60)))||cast(',' as char(1))||
	trim(cast(Gate_ID as VARCHAR(100)))||cast(',' as char(1))||
	trim(cast(Station_Hall_ID as VARCHAR(60)))||cast(',' as char(1))||
	trim(cast(Station_ID as VARCHAR(30)))||cast(',' as char(1))||
	trim(cast(Line_ID as VARCHAR(30)))||cast(',' as char(1))||
	trim(cast(Trip_Drct_Cd as CHAR(2)))||cast(',' as char(1))||
	trim(cast(Data_Stat_Std_Cd as CHAR(2)))||cast(',' as char(1))||
	trim(cast(Data_Dt as VARCHAR(20)))||cast(',' as char(1))||
	trim(cast(cast(Entry_Quatity as number(18)) as VARCHAR(20))) ||cast(',' as char(1))||
	trim(cast(cast(Exit_Quatity as number(18)) as VARCHAR(20))) ||cast(',' as char(1))||
	trim(cast(cast(Swipe_Card_Entry_Quatity as number(18)) as VARCHAR(20))) ||cast(',' as char(1))||
	trim(cast(cast(Swipe_Card_Exit_Quatity as number(18)) as VARCHAR(20))) as char(500)		
)
from		BMNC_Pmart.t98_pasgr_date_st
where	data_dt = cast('${tx_date}' as date format 'yyyymmdd')-7 
and Data_Stat_Std_Cd='02'
;

.END EXPORT;
.LOGOFF;

ENDOFINPUT

   close(FEXP);
   my $FEXP_Code = $? >> 8;

    if ($FEXP_Code) {
    	print "Failed to call fastexport!FEXP_Code=$FEXP_Code\n";
    	return $FEXP_Code;
    }
    ### adjust the outfile content.
    unless ( open(FH, "/home/etl/DATA/process/$tmpfile") ) {
    	print "Failed to open data file $tmpfile!\n";
        return 1;
    }
    unless ( open(TFH, ">/home/etl/DATA/process/$outfile") ) {
    	print "Failed to open data file $outfile!\n";
        return 1;
    }  
    print "Now copying...\n";
    my $oneLine; 
    while ($oneLine = <FH>) {
    	chomp($oneLine);
    	$oneLine =~ s/(\s+)$//;
    	print TFH "$oneLine\n";
	   	#print "###$oneLine###\n";
    }
    close(FH);
    close(TFH);
print "End format /home/etl/DATA/process/$outfile...\n";


#if(-e "$home/$tmpfile"){
#   `rm -f $home/$tmpfile`;
#   print "temp file delete sucess!\n";
#}

print "***************EXPORT FILE SUCCESS !*******************\n";

my $ftp=Net::FTP->new("10.254.52.8") or die  "error";
$ftp->login("etl","etl123");
$ftp->cwd("/data/etl/yangm/exp");
$ftp->put("/home/etl/DATA/process/t98_pasgr_date_st${tx_date}.csv");   #�����е�csv�ϴ���ftp

print "***************t98_pasgr_date_st${tx_date}.csv FTP success !*******************\n";

system(" touch  /home/etl/DATA/process/exp_t98_pasgr_date_st$tx_date.dir");   #�ϴ���ɺ�ɾ��
my $aa = `wc -lm /home/etl/DATA/process/t98_pasgr_date_st${tx_date}.csv`;
my @arr = split(" ",$aa);
my @arr1 = split("\/",$arr[2]);
my $fileName=$arr1[5];
my $lines=$arr[0];
my $sizes = $arr[1];
open(FH,">/home/etl/DATA/process/exp_t98_pasgr_date_st${tx_date}.dir") or die $!;
print FH "$fileName\t";
print FH "$sizes\t";
print FH "$lines\n";
close(FH);

#my $ftp=Net::FTP->new("10.254.52.8") or die  "error";
#$ftp->login("etl","etl123");
#$ftp->cwd("/data/etl/yangm/exp");
$ftp->put("/home/etl/DATA/process/exp_t98_pasgr_date_st$tx_date.dir");
$ftp->quit;

system("rm /home/etl/DATA/process/$tmpfile");
system("rm /home/etl/DATA/process/$outfile");
system("rm /home/etl/DATA/process/exp_t98_pasgr_date_st$tx_date.dir");

print "***************script execute end !*******************\n";
