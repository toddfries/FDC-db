# Copyright (c) 2005-2008 Todd T. Fries <todd@fries.net>
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

use strict;
use warnings;

package FDC::db;

use strict;
use warnings;

use POSIX qw(strftime);
use Term::ReadKey;

use DBI;

use DBI::Const::GetInfoType qw(%GetInfoType);

use strict;
use warnings;

sub new {
	my ($class, $dsn, $user, $pass) = @_;
	my $me = { name => 'FDC::db' };

	if (!defined($user) || length($user) == 0) {
		if (defined($ENV{'PGUSER'})) {
			$user = $ENV{'PGUSER'};
		} else {
			chomp($user = `id -un`);
		}
	}

	$me->{dsn} = $dsn;
	$me->{user} = $user;
	$me->{pass} = $pass;

	my $ret = bless $me, $class;
	if ($me->connectloop(10)) {
		return undef;
	}
	$me->{dbmsname} = $me->{dbh}->get_info( $GetInfoType{SQL_DBMS_NAME} );

	return $ret;
}
sub
oidname
{
	my ($me) = @_;

	if ($me->{dbmsname} eq "PostgreSQL") {
		return "oid";
	}
	if ($me->{dbmsname} eq "SQLite") {
		return "rowid";
	}
	printf STDERR "Oops, unsupported database: '%s'\n", $me->{dbmsname};
	return undef;
}

sub
_debug
{
	my ($me) = @_;
	if (defined($ENV{'fdct_debug'})) {
		if ($ENV{'fdct_debug'} eq "on") {
			return 1;
		}
	}
	return 0;
}

# Low level routines with convenient error checking

sub
connectloop
{
	my ($me,$loopcount) = @_;
	my $count = 0;
	while ($me->connect) {
		printf STDERR "Connect attempt #%d of %d to db %s failed.\n",
		    $count++, $loopcount, $me->{dsn};
		if ($count > $loopcount) {
			return 1;
		}
		sleep(1);
	}
	return 0;
}

sub
connect
{
	my ($me) = @_;
	my $dbh;
	my ($dsn,$user,$pass) = ($me->{dsn},$me->{user},$me->{pass});

# XXX set AutoCommit = 0 in the future
# XXX consider RaisError = 1, will exit script if errors occur, ?? desirable ??

	retry:
	if ($me->_debug) {
		print "dsn: $dsn, user: $user, pass: $pass\n";
	}
	eval {
		$dbh = DBI->connect($dsn, $user, $pass,
	   	    { RaiseError => 1, AutoCommit => 1, PrintError => 0});
	};
	if ($@ || !defined($dbh) || $dbh == -1) {
		if ($@ =~ /krb5_cc_get_principal: No such file/) {
			system("kinit");
			goto retry;
		}
		if ($@ =~ /no pg_hba.conf entry for host/) {
			print "FATAL: EPERM. $@";
			exit(1);
		}
		if ($@ =~ /Can.t locate .*pm in \@INC/) {
			print "";
		}
		if ($@ =~ /no password supplied/ ||
		    $@ =~ /password authentication failed/) {
			my $pghost = "";
			if (defined($ENV{'PGHOST'})) {
				$pghost = "host=".$ENV{'PGHOST'};
			}
			printf "dsn=%s%s,user=%s password: ", $dsn, $pghost, $user;
			ReadMode('noecho');
			my $str = <STDIN>;
			ReadMode(0);
			print "\n";
			chomp($pass = $str);
			$me->{pass} = $pass;
			goto retry;
		}
		print STDERR $me->issuestr($@, "new($dsn,USER,PASS)");
			
		return 1;
	}
	$me->{dbh} = $dbh;
	return 0;
}

sub getdbh {
	my ($me) = @_;
	if (!defined($me->{dbh})) {
		if ($me->connectloop(10)) {
			return undef;
		}
	}
	return $me->{dbh};
}

#
# query to return the 1st result
#

sub query1 {
	my ($me, $query) = @_;

	my ($sth);

	if (! ($sth = $me->doquery($query, 'query1'))) {
		return -1;
	}

	my ($ret) = $sth->fetchrow_array;
	if ( !defined($sth) || $sth == -1) {
		return -1;
	}
	$sth->finish;

	return $ret;
}

sub issuestr {
	my ($me, $at, $funcinfo) = @_;
	my $str = "";
	if (length($at) > 0) {
		$str = "$at\n";
	}
	$str .= sprintf "Issue..%s \n",$funcinfo;
	if ($me->_debug) {
		$str .= sprintf "at = %s ..\n",$at;
	}
	my ($err,$errstr,$state);
	if (defined($me->{dbh})) {
		$err = $me->{dbh}->err;
		$errstr = $me->{dbh}->errstr;
		$state = $me->{dbh}->state;
	} else {
		if (defined($DBI::err)) {
			$err = $DBI::err;
			$errstr = $DBI::errstr;
			$state = $DBI::state;
		}
	}
	if (defined($err)) {
		$str .= sprintf "err = %d, errstr = %s\n",$DBI::err,
		    $DBI::errstr;
		$str .= sprintf "state = %s\n", $DBI::state;
	}
	return $str;
}
	

#
# query to return one result or fail
#
sub do_oneret_query {
	my ($me, $query) = @_;

	my $sth;

	eval {
		$sth = $me->doquery($query, 'do_oneret_query');
	};
	if ($@) {
		print STDERR $me->issuestr($@, "do_oneret_query($query)");
		return -1;
	}
	if ( !defined($sth) || $sth == -1) {
		print STDERR $me->issuestr("", "do_oneret_query($query)");
		return -1;
	}

	# is $sth->rows not reliable on SQLite ?
	#if ($sth->rows != 1) {
	#	return -1;
	#}
	my (@ret) = $sth->fetchrow_array;
	if ($#ret < 0) {
		return -1;
	}
	if (!defined($ret[0])) {
		return -1;
	}
	$sth->finish;
	if ($me->_debug) {
		printf STDERR "do_oneret_query %s\n",$ret[0];
	}

	return $ret[0];
}

sub prepare {
	my ($me, $query, $caller) = @_;

	if (!defined($caller)) {
		$caller = "";
	}

	my $sth;
	my $dbh = $me->getdbh;
	eval {
		$sth = $dbh->prepare($query);
	};
	if ($@) {
		print STDERR $me->issuestr($@, "doquery($query,$caller):prepare");
		if ($me->_debug) {
			printf STDERR "[%s] failed to prepare\n",$query;
		}
		if ($dbh->state =~ m/(57P01|800[06])$/) {
			printf STDERR "[$query] lost connection to db, retry\n";
			$dbh->disconnect;
			if ($me->connectloop(10)) {
				exit(1);
			}
			# XXX infinite loop or not?
			return $me->prepare($query,$caller);
		}
		return undef;
	}
	my $newsth = FDC::db::sth->new($me, $sth, $query, "FDC::db($caller)");
	return $newsth;
}


#
# query to return multiple results
#
sub doquery {
	my ($me, $query, $caller) = @_;
	my ($sth,$rv);
	if (!defined($caller)) {
		$caller = "";
	}

	if ($me->_debug) {
		printf STDERR "prepare[%s] %s\n", $query, $caller;
	}

	my ($dbh) = $me->getdbh;
	
	$sth = $me->prepare($query, "doquery($query,$caller)");
	if (!defined($sth)) {
		return -1;
	}
	$rv = $sth->execute(undef, "doquery([$query],$caller)");
	$rv = $sth->rows;

	if ( $rv < 0 ) {
		$sth->finish;
		if ($me->_debug) {
			printf STDERR "[$query] returned $rv rows\n";
			STDERR->flush;
		}
		return -1;
	}

	return $sth;
}

sub do_oid_insert {
	my ($me, $query, $caller) = @_;

	if($me->_debug) {
		printf STDERR "do_oid_insert($me,'$query','$caller')\n";
	}

	my ($sth);

	$sth = $me->doquery($query, 'do_oid_insert') || return -1;
	if (!defined($sth) || $sth eq -1) {
		return -1;
	}
	if (! ($query =~ /^insert /i)) {
		return -1;
	}
	my $table = $query;
	$table =~ / into ([^ ]+) /i;
	$table = $1;
	if (!defined($table)) {
		printf STDERR "do_oid_insert: no table found in query '%s'\n", $query;
		$table = "";
	}
	my ($oid) = $sth->getoid($table);
	if($me->_debug) {
		printf STDERR "do_oid_insert returning oid = $oid;\n";
	}

	$sth->finish;
	return $oid;
}

sub quote {
	my ($me, $str) = @_;

	return $me->getdbh->quote($str);
}

sub tables {
	my ($me) = @_;

	return $me->getdbh->tables();
}

package FDC::db::sth;

use strict;
use warnings;

# Two groups of functions
# 1. Utility functions unique to FDC::db::sth
# 2. Functions to mimic the sth class returned by $dbh->prepare()


#
# Utility functions
#

sub new {
	my ($class, $db, $sth, $query, $caller) = @_;
	my $me = { name => 'sth' };

	$me->{db}  = $db;
	$me->{sth} = $sth;
	$me->{query} = $query;
	$me->{caller} = $caller;
	@{$me->{bindparams}} = ();

	my $ret = bless $me, $class;

	return $ret;
}

sub getsth {
	my ($me) = @_;
	if (!defined($me->{sth})) {
		return undef;
	}
	return $me->{sth};
}

sub getoid {
	my ($me, $table) = @_;
	if (!defined($me->{sth})) {
		printf STDERR "Warning: getoid() called but me->{sth}==undef\n";
		return undef;
	}

	my $dbmsname = $me->{db}->{dbmsname};
	if ($dbmsname eq "PostgreSQL") {
		my $oidss = $me->{sth}->{pg_oid_status}; # oid
		my $oidsd = $me->{db}->{dbh}->{pg_oid_status}; # oid
		my $oidli;
		eval {
			$oidli = $me->{db}->{dbh}->last_insert_id("","","$table",""); # autoincrement column number
		};
		if ($@) {
			printf STDERR "last_insert_id: $@";
			$oidli = $me->{db}->do_oneret_query("select max(id) from $table;");
		}
		return $oidli;
	}
	if ($dbmsname eq "SQLite") {
		my $oidli = $me->{db}->{dbh}->last_insert_id("","","$table","");
		return $oidli;
	}
	if ($dbmsname eq "mysql") {
		my $oidsd = $me->{db}->{dbh}->{mysql_insertid}; # autoincrement column number
		my $oidli = $me->{db}->{dbh}->last_insert_id("","","$table",""); # autoincrement column number
		return $oidli;
	}
	printf STDERR "getoid: Unsupported DBMS Name: '%s'\n", $dbmsname;
	return $me->{db}->{dbh}->last_insert_id("","","$table","");
}

#
# sth mimic functions, handle errors gracefully
#

sub fetchrow_array {
	my ($me) = @_;

	# XXX observed to fail
	return $me->getsth->fetchrow_array;
}
sub fetch {
	my ($me) = @_;

	# XXX observed to fail
	return $me->getsth->fetch;
}
sub finish {
	my ($me) = @_;
	return $me->getsth->finish;
}
sub rows {
	my ($me) = @_;

	return $me->getsth->rows;
}
sub fetchrow_arrayref {
	my ($me) = @_;

	return $me->fetch;
}
sub fetchall_arrayref {
	my ($me, $slice, $max_rows) = @_;

	return $me->getsth->fetchall_arrayref($slice, $max_rows);
}
sub bind_param {
	my ($me, $count, $info, $type) = @_;
	my $bindparm;
	@{$bindparm} = ($count, $type, $info);

	push @{$me->{bindparams}}, $bindparm;
	if ($me->{db}->_debug) {
		printf STDERR "# %s FDC::db::sth->bind_param(%s,%s,%s) [%s]\n",
	    $me, $count, (length($info) > 1024) ? "length(\$info)=".length($info) : $info, $type, $me->{query};
	}

	return $me->getsth->bind_param($count, $info, $type);
}

sub execute {
	my ($me, $query, $caller) = @_;
	my $rv;
	my $db = $me->{db};
	my $sth = $me->getsth;
	if (!defined($caller)) {
		$caller = "";
	}
	eval {
		if (defined($query)) {
			$rv = $sth->execute($query);
		} else {
			$rv = $sth->execute;
		}
	};
	if ($@) {
		printf STDERR "# %s FDC::db::sth: $@", $me;
		printf STDERR "# %s FDC::db::sth: query='%s'\n",$me, $query;
		my ($x,$y,$z);
		for my $parm (@{$me->{bindparams}}) {
			($x,$y,$z) = @{$parm};
			if (length($y) > 1024) {
				$y = "length()=".length($y);
			}
			printf STDERR "# %s FDC::db::sth: ", $me;
			printf STDERR "bind_params(%s,%s,%s)\n",$me,$x,$y,$z;
		}
		if ($me->{db}->_debug) {
			printf STDERR "[$query] failed, returned $rv\n";
			STDERR->flush;
		}
		print STDERR $me->{db}->issuestr($@, "$caller");

		my $lostdb = 0;
		my $retry = 0;

		# PostgreSQL error codes:
		#   8000 = timed out
		#   8006 = disconnected
		#   57P01 = remote system shutdown
		if ($db->getdbh->state =~ /(57P01|800[06])/) {
			$lostdb = 1;
			$retry = 1;
		}

		# SQLite3 error codes:
		# S1000 = generic error, must also parse errstr
		if ($db->getdbh->state =~ /S1000/) {
			#$lostdb = 0;
			$lostdb = 1;
			$retry = 1;
		}
		if ($lostdb == 1) {
			printf STDERR "FDC::db::sth: lost connection to db [%s]\n",$query;
			if ($db->connectloop(10)) {
				exit(1);
			}
		}
		if ($retry == 1) {
			printf STDERR "FDC::db::sth: retrying query [%s]\n",$query;
			$sth = $db->prepare($query, $caller);
			if (!defined($sth)) {
				exit(1);
			}
			my @parms = @{$me->{bindparams}};
			@{$me->{bindparams}} = ();
			foreach my $parm (@parms) {
				my ($count, $type, $info) = @{$parm};
				printf STDERR "# %s (%s,%s,%s)\n", $me,
				    $count, $info, $type;
				$me->bind_param($count,$info,$type);
			}
			$me->{sth} = $sth;
			return $me->execute(undef, $caller);
		}
		return -1;
	}
	# Clear parms after success, right?? ;-)
	@{$me->{bindparams}} = ();
	return $rv;
}

1;
