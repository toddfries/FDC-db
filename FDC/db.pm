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

package FDC::db;

use POSIX qw(strftime);

use DBI;
#use Date::Manip;

sub new {
	my ($class, $dsn, $user, $pass) = @_;
	my $self = { name => 'dbh' };

# XXX set AutoCommit = 0 in the future
# XXX consider RaisError = 1, will exit script if errors occur, ?? desirable ??

	if ($ENV{'fdct_debug'} eq "on") {
		print "dsn: $dsn, user: $user, pass: $pass\n";
	}
	$self->{dbh} = DBI->connect($dsn, $user, $pass,
	   { RaiseError => 0, AutoCommit => 1}) or die $DBI::errstr;

	$self->{dsn} = $dsn;
	$self->{user} = $user;
	$self->{pass} = $pass;

	bless $self, $class;
}

sub getdbh {
	my ($self) = @_;
	return $self->{dbh};
}

#
# query to return one result
#

sub query1 {
	my ($self, $query) = @_;

	my ($sth);

	if ( ($sth = $self->doquery($query, 'query1')) == -1 ) {
		return -1;
	}

	my ($ret) = $sth->fetchrow_array;
	$sth->finish;

	return $ret;
}

#
# query to return multiple results
#
sub doquery {
	my ($self, $query, $caller) = @_;
	my ($sth,$rv);

	if ($ENV{'fdct_debug'} eq "on") {
		printf STDERR "prepare(%s) %s\n", $query, $caller;
	}

	my ($dbh) = $self->getdbh;

	if (! ($sth = $dbh->prepare($query))) {
		if ($ENV{'fdct_debug'} eq "on") {
			printf STDERR "[%s] failed to prepare, returned undef\n",$query;
		}
		return -1;
	}
		
	if (! ($rv = $sth->execute) ) {
		if ($ENV{'fdct_debug'} eq "on") {
			printf STDERR "[$query] failed, returned $rv\n";
		}
		return -1;
	}
	$rv = $sth->rows;

	if ( $rv < 0 ) {
		$sth->finish;
		if ($ENV{'fdct_debug'} eq "on") {
			printf STDERR "[$query] returned $rv rows\n";
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

	my ($oid) = $sth->{pg_oid_status};

	$sth->finish;
	return $oid;
}

sub quote {
	my ($self, $str) = @_;

	return $self->{dbh}->quote($str);
}

1;
