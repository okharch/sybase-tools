#!/usr/bin/env perl
use strict;
use warnings;
use DBIx::Brev;
use Config::General;
use Getopt::Long;
use Pod::Usage;
use Sereal::Encoder qw(encode_sereal);
use Sereal::Decoder qw(decode_sereal);
use File::Slurp qw(read_file write_file);

$\="\n";

my $HOME = $ENV{HOME};
my ($s) = $0 =~ m{^(?:.*/)?(.*?)(?:\..*)?$}; # get script name, truncate path and ext
my ($config_file) = grep -f $_,map "$_/$s.conf",$HOME,"$HOME/etc","/etc";
my %config = Config::General->new($config_file)->getall if $config_file;

my ($server,$username,$password,$data_source,$exclude_db) = @config{qw(sybase_server username password data_source exclude_db)};
my $refresh_data = 0;
my $dbs = '';
my $help = 0;
 
GetOptions(
	'sybase_server=s' => \$server,
	'data_source=s' => \$data_source, 
	'username=s' => \$username, 
	'password=s' => \$password,
	'refresh_data' => \$refresh_data,
	'exclude_db=s' => \$exclude_db,
	'db=s' => \$dbs,
	'help' => \$help,
);
pod2usage(-verbose  => 2) if $help;

my @columns_re = @ARGV; # we can search table for multiple columns at once

# now load cached data if any
my ($dat_dir) = grep -d, "$HOME/etc", $HOME;
my ($dserver) = grep $_, $server, $data_source =~ m/dbi:Sybase:([A-Za-z_0-9]+)/;
my $dat_file = "$dat_dir/fcol-$dserver.dat";
$refresh_data = 1 unless $refresh_data || -f $dat_file;
my $data = {};
if (-f $dat_file) {
	my $buf = read_file( $dat_file, binmode => ':raw' ) ;
	$data = decode_sereal($buf);
}

# find out the list of databases to work with
my @dbs = split /,/,$dbs;
@dbs = $data?keys %$data:() unless @dbs;
unless (@dbs) {
	db_connect();
	@dbs = map $_->{database_name}, sql_query_hash qq{exec sp_databases};
}

my %exclude_db = map {$_ => undef} split /,/,$exclude_db if $exclude_db;
@dbs = grep !exists $exclude_db{$_}, @dbs;

# refresh data if no data at all or $refresh_data
my $updated = 0;
for my $db (@dbs) {	
	delete $data->{$db} if $refresh_data;
	unless ($data->{$db}) {
		$data->{$db} = scan_db_columns($db);
		$updated++ if $data->{$db};
	}
}

$updated = 1 if grep exists $data->{$_}, keys %exclude_db;

# save data file if updated
if ($updated) {
	print "\nupdating $dat_file...";
	delete @{$data}{keys %exclude_db};
	my $buf = encode_sereal($data);
	write_file( $dat_file, {binmode => ':raw'}, $buf );
}

pod2usage("$0: No columns to look for given.") unless (@columns_re);

# now when we have columns scan over all tables for specified columns
for my $db (sort @dbs) {	
	my $tables = $data->{$db};
	next unless $tables;
	TABLE:
	for my $table (sort keys %$tables) {
		my @tcolumns = @{$tables->{$table}};
		# find if some set of columns match to all specified templates
		my %matched_columns;
		for my $re (@columns_re) {
			my @m = grep m{$re}i, @tcolumns;
			next TABLE unless @m;
			@matched_columns{@m} = ();
		}
		printf "$db..$table : %s\n",join(", ",sort keys %matched_columns);
	}
}

sub scan_db_columns {
	my ($db) = @_;
	db_connect();
	eval {
	sql_exec "use $db";
	1;
	} or do {
		print "database $db can't be scanned with configured user";
		return;
	};
	print "scanning database $db...";
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
	print "Connecting to database...";
	$data_source = "dbi:Sybase:$server" unless $data_source;
	$data_source =~ s/(?<=dbi:Sybase:)[A-Za-z_0-9]+/$server/ if $server; # overwrite sybase server in datasource if specified
	my @options = (username=>$username,password=>$password) if $username;
	db_use($data_source,@options);
	$connected = 1;
}

__END__
=head1 NAME
fcol.pl - Tool to find columns in sybase database quickly

=head1 SYNOPSIS
fcol.pl [options] col1 col2

Options:
	-sybase_server $server to override server specified in data_source and/or in config file
	-data_source dbi:Sybase:SYBASE_INTERFACE_REFERENCE
	-username=$username user for auth
	-password=$password password for auth
	-refresh_data 	to refresh data for list of database in scope
	-exclude_db=s   to ovveride list of databases specified in config
	-db=s			to set list of databases explicitly rather than all databases from server.
	
)

=head1 DESCRIPTION

It caches sybase metadata in ~/etc/fcol.dat file and does not require db connect after that.
You can put fcol.conf with config values to either $HOME/etc or $HOME directory.
Usually you want put there following keys:

 data_source=dbi:Sybase:SYBASE_SERVER_INT_KEY
 username=user
 password=pass123
 exclude_db=model,core_model,core_model2,sbcsecurity,fir_load_status,mda_archive,risk_control,dbccdb,dprsdb
 
Usually you want to put comma separated list of databases which user can't access to exclude_db.
Otherwise it will be clucking all the time about failing on connection to those databases.
Then you use 

fcol.pl col1 col2 col3
to show list of tables which contains all the specified columns.
Also you can override any config keys with command line options of the same name.

If you want to refresh cached meta data best solution is just rm ~/etc/fcol.dat.

Enjoy!
=cut
