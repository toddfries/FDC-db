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

use POSIX qw(strftime);

use DBI;

sub new {
	my ($class, $dsn, $user, $pass) = @_;
	my $self = { name => 'dbh' };

	$self->{dsn} = $dsn;
	$self->{user} = $user;
	$self->{pass} = $pass;

	my $ret = bless $self, $class;
	if ($self->connectloop(10)) {
		return undef;
	}
	return $ret;
}

# Low level routines with convenient error checking

sub connectloop {
	my ($self,$loopcount) = @_;
	my $count = 0;
	while ($self->connect) {
		printf STDERR "Connect attempt #%d of %d to db %s failed.\n",
		    $count++, $loopcount, $self->{dsn};
		if ($count > $loopcount) {
			return 1;
		}
		sleep(1);
	}
	return 0;
}

sub connect {
	my ($self) = @_;
	my $dbh;
	my ($dsn,$user,$pass) = ($self->{dsn},$self->{user},$self->{pass});

# XXX set AutoCommit = 0 in the future
# XXX consider RaisError = 1, will exit script if errors occur, ?? desirable ??

	if ($ENV{'fdct_debug'} eq "on") {
		print "dsn: $dsn, user: $user, pass: $pass\n";
	}
	eval {
		$dbh = DBI->connect($dsn, $user, $pass,
	   	    { RaiseError => 1, AutoCommit => 1, PrintError => 0});
	};
	if ($@ || !defined($dbh) || $dbh == -1) {
		print STDERR issuestr($@, "new($dsn,USER,PASS)");
		return 1;
	}
	$self->{dbh} = $dbh;
	return 0;
}

sub getdbh {
	my ($self) = @_;
	if (!defined($self->{dbh})) {
		if ($self->connectloop(10)) {
			return undef;
		}
	}
	return $self->{dbh};
}

#
# query to return the 1st result
#

sub query1 {
	my ($self, $query) = @_;

	my ($sth);

	if (! ($sth = $self->doquery($query, 'query1'))) {
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
	my ($at,$funcinfo) = @_;
	my $str = "";
	if (length($at) > 0) {
		$str = "$at\n";
	}
	$str .= sprintf "Issue..%s \n",$funcinfo;
	if ($ENV{'fdct_debug'} eq "on") {
		$str .= sprintf "at = %s ..\n",$at;
	}
	$str .= sprintf "err = %d, errstr = %s\n",$DBI::err,
	    $DBI::errstr;
	$str .= sprintf "state = %s\n", $DBI::state;
	return $str;
}
	

#
# query to return one result or fail
#
sub do_oneret_query {
	my ($self, $query) = @_;

	my ($sth);

	eval {
		$sth = $self->doquery($query, 'do_oneret_query');
	};
	if ($@) {
		print STDERR issuestr($@, "do_oneret_query($query)");
		return -1;
	}
	if ( !defined($sth) || $sth == -1) {
		print STDERR issuestr("", "do_oneret_query($query)");
		return -1;
	}

	if ($sth->rows != 1) {
		return -1;
	}
	my ($ret) = $sth->fetchrow_array;
	$sth->finish;
	if ($ENV{'fdct_debug'} eq "on") {
		printf STDERR "do_oneret_query %s\n",$ret;
	}

	return $ret;
}

sub prepare {
	my ($self, $query, $caller) = @_;

	my $sth;
	my $dbh = $self->getdbh;
	eval {
		$sth = $dbh->prepare($query);
	};
	if ($@) {
		print STDERR issuestr($@, "doquery($query,$caller):prepare");
		if ($ENV{'fdct_debug'} eq "on") {
			printf STDERR "[%s] failed to prepare\n",$query;
		}
		if ($dbh->state =~ m/8006$/) {
			printf STDERR "[$query] lost connection to db, retry\n";
			$dbh->disconnect;
			if ($self->connectloop(10)) {
				exit(1);
			}
			# XXX infinite loop or not?
			return $self->prepare($query,$caller);
		}
		return undef;
	}
	return $sth;
}

sub execute {
	my ($self, $sth, $query, $caller) = @_;
	my $rv;
	my $dbh = $self->getdbh;
	eval {
		$rv = $self->execute($sth);
	};
	if ($@) {
		printf STDERR "[$query]: $@\n";
		if ($ENV{'fdct_debug'} eq "on") {
			printf STDERR "[$query] failed, returned $rv\n";
			STDERR->flush;
		}
		print STDERR issuestr($@, "$caller");
		if ($dbh->state eq "S8006") {
			printf STDERR "[$query] lost connection to db, bail\n";
			# in order to reconnect we'd also have to store
			# the last prepare and re-prepare after connection
			# succeeded
			exit(1);
		}
		return -1;
	}
	return $rv;
}

#
# query to return multiple results
#
sub doquery {
	my ($self, $query, $caller) = @_;
	my ($sth,$rv);

	if ($ENV{'fdct_debug'} eq "on") {
		printf STDERR "prepare[%s] %s\n", $query, $caller;
	}

	my ($dbh) = $self->getdbh;
	
	$sth = $self->prepare($query, "doquery($query,$caller)");
	if (!defined($sth)) {
		return -1;
	}
	$rv = $self->execute($sth, $query, "doquery(..,$caller)");
	$rv = $sth->rows;

	if ( $rv < 0 ) {
		$sth->finish;
		if ($ENV{'fdct_debug'} eq "on") {
			printf STDERR "[$query] returned $rv rows\n";
			STDERR->flush;
		}
		return -1;
	}

	return $sth;
}

sub do_oid_insert {
	my ($self, $query, $caller) = @_;

	if($ENV{'fdct_debug'} eq "on") {
		printf STDERR "do_oid_insert(,'$query','$caller')\n";
	}

	my ($sth);

	$sth = $self->doquery($query, 'do_oid_insert') || return -1;
	if (!defined($sth) || $sth eq -1) {
		return -1;
	}
	my ($oid) = $sth->{pg_oid_status};
	if($ENV{'fdct_debug'} eq "on") {
		printf STDERR "do_oid_insert return oid = $oid;\n";
	}

	$sth->finish;
	return $oid;
}

sub quote {
	my ($self, $str) = @_;

	return $self->getdbh->quote($str);
}

1;

