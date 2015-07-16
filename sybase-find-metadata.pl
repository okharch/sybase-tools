#!/usr/bin/env perl

# (C) 2013-2015 Oleksandr Kharchenko okharch@gmail.com, UBS
# look for the latest version at https://github.com/okharch/sybase-tools/blob/master/sybase-find-metadata.pl

use strict;
use warnings;
use DBIx::Brev;
use Config::General;
use Getopt::Long;
use Pod::Usage;
use Sereal::Encoder qw(encode_sereal);
use Sereal::Decoder qw(decode_sereal);
use File::Slurp qw(read_file write_file);

my ($server,$username,$password,$exclude_db,$case_sensitive,$data_dir);
my ($refresh_data,$dbs,$help,$look_for_table,$verbose) = (0,'',0,0,0);
my ($dat_file);

init_parameters(); 

my @columns_re = @ARGV; # we can search table for multiple columns at once
$refresh_data = 1 unless $refresh_data || -f $dat_file;
my $data = list_dbs();

pod2usage("$0: No columns to look for given.") unless (@columns_re);

# now when we have columns scan over all tables for specified columns
my @dbs = sort keys %$data;
printf STDERR "scaning %d databases for matches : %s\n",scalar(@dbs),join(" & ", @columns_re) if $verbose;
for my $db (@dbs) {	
	my $tables = $data->{$db};
	next unless $tables;
    show_matches($db,$tables,$_) for (sort keys %$tables);
}
exit 0;

sub show_matches {
    my ($db,$tables,$table) = @_;
    # show the table name if it matches all the regs
    printf "%s..%s\n",$db,$table if grep({$case_sensitive?$table =~ m{$_}:$table =~ m{$_}i} @columns_re) == @columns_re;
    return if $look_for_table;
    my @tcolumns = @{$tables->{$table}};
    # find if some set of columns match to all specified templates
    my %matched_columns;
    for my $re (@columns_re) {
        my @m = grep {$case_sensitive?m{$re}:m{$re}i} @tcolumns;
        return unless @m;
        @matched_columns{@m} = ();
    }
    printf "$db..$table : %s\n",join(", ",sort keys %matched_columns);
}

sub scan_db_columns {
	my ($db) = @_;
	db_connect();
	eval {
	sql_exec "use $db";
	1;
	} or do {
		print STDERR "database $db can't be scanned with configured user";
		return;
	};
	print STDERR "scanning database $db..." if $verbose;
	my @data = sql_query q{
SELECT table_name=sysobjects.name, column_name=syscolumns.name 
FROM sysobjects INNER JOIN syscolumns ON sysobjects.id = syscolumns.id 
where sysobjects.type='U' 
	};
	my %tables;
	for (@data) {
		my ($table,$column) = @$_;
		push @{$tables{$table}}, $column;
	}
	return \%tables;
}

my $connected;
sub db_connect {
	return if $connected;
	print STDERR "Connecting to database..." if $verbose;
	my $data_source = "dbi:Sybase:$server";
	my @options = (username=>$username,password=>$password) if $username;
	db_use($data_source,@options);
	$connected = 1;
}

sub init_parameters {
    $\="\n";
    my $HOME = $ENV{HOME};
    my ($s) = $0 =~ m{^(?:.*/)?(.*?)(?:\..*)?$}; # get script name, truncate path and ext
    my ($config_file) = grep -f $_,map "$_/$s.conf",$HOME,"$HOME/etc","/etc";
    my %config = Config::General->new($config_file)->getall if $config_file;
    ($server,$username,$password,$exclude_db,$case_sensitive,$data_dir) = @config{qw(sybase_server username password exclude_db case_sensitive data_dir)};
    ($data_dir) = grep -d, "$HOME/etc", $HOME unless $data_dir;
    GetOptions(
        'sybase_server=s' => \$server,
        'server=s' => \$server,
        'username=s' => \$username, 
        'password=s' => \$password,
        'refresh_data' => \$refresh_data,
        'exclude_db=s' => \$exclude_db,
        'db=s' => \$dbs,
        'table' => \$look_for_table,
        'help' => \$help,
        'verbose' => \$verbose,
        'data_dir=s' => \$data_dir,
    );
    pod2usage(-verbose  => 2) if $help;
    pod2usage("$0: No server was specified") unless ($server);
    pod2usage("$0: No data_dir has been specified") unless ($data_dir);
    pod2usage("$0: No credentials for sybase server were specified") unless ($username && $password);
    # now load cached data if any
    $dat_file = "$data_dir/sybase-md-$server.dat";
}

# find out the list of databases to work with
sub list_dbs {
my $data = {};
if (!$refresh_data && -f $dat_file) {
	my $buf = read_file( $dat_file, binmode => ':raw' ) ;
	$data = decode_sereal($buf);
}
my @dbs = split /,/,$dbs;
@dbs = $data?keys %$data:() unless @dbs;
unless (@dbs) {
	db_connect();
	@dbs = map $_->{database_name}, sql_query_hash qq{exec sp_databases};
}

my %exclude_db = map {$_ => undef} split /,/, $exclude_db if $exclude_db;
@dbs = sort grep( !exists $exclude_db{$_}, @dbs );

# refresh data if no data at all or $refresh_data
my $updated = 0;
for my $db (@dbs) {	
	delete $data->{$db} if $refresh_data;
	unless ($data->{$db}) {
        my $tables = scan_db_columns($db);
		$data->{$db} = $tables if $tables;
		$updated++ if $data->{$db};
	}
}

$updated = 1 if grep exists $data->{$_}, keys %exclude_db;

# save data file if updated
if ($updated) {
	print STDERR "\nupdating $dat_file..." if $verbose;
	delete @{$data}{keys %exclude_db};
	my $buf = encode_sereal($data);
	write_file( $dat_file, {binmode => ':raw'}, $buf );
}

# exit silently if -r option and no columns to look for given
exit 0 if $refresh_data && !@columns_re;

return $data;
}
__END__
=head1 NAME
sybase-find-metadata.pl - Tool to find columns in sybase database quickly

=head1 SYNOPSIS
sybase-find-metadata.pl [options] col1 [col2]

Options:
	
    -sybase_server or -server $server to override server specified in data_source and/or in config file	
    
    -username=s     user for auth
	
    -password=s     password for auth
    
    -data_dir=s     this directory is used to put cached metadata for sybase server
	
    -exclude_db=s   it does not try list metadata for specified databases. the default behaviour is to try to discover metadata of every database at server
	
    -db=s           to set list of databases explicitly rather than all databases from server.
    
    -refresh_data   to refresh data for current server
	
    -table          look only for table names for specified pattern(s), avoid columns scanning 
	
    -verbose        explain what it does to STDERR. otherwise it only shows if something has been found
    
    -help           display this help screen
	
)

=head1 DESCRIPTION

It caches sybase metadata in $data_dir/sybase-md-$server.dat file and does not require db connect after that.
You can put sybase-find-metadata.conf with config values to either $HOME/etc or $HOME directory.
Usually you want put there following keys:

 username=user
 password=pass123
 
As this is not a good idea to specify credentials at command line.
All other parameters you can use at command line and create alias for convenience:
alias fcol='sybase-find-metadata.pl -server SM_FIADEV1_SQL -data_dir /sbcimp/dyn/data/mbs/etc'
 
Then you use 

fcol col1 col2 col3
to show list of tables which contains all the specified columns.
Also you can override any config keys with command line options of the same name.

If you want to refresh cached meta data best solution is just rm ~/etc/fcol.dat.

Enjoy!
=cut
