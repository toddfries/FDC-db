use ExtUtils::MakeMaker;

WriteMakefile( NAME => "FDC",
	VERSION_FROM => 'lib/FDC/db.pm',
	PREREQ_PM => {
		'DBI' => 0,
		'Term::ReadKey' => '0',
	},
	EXE_FILES => [],
	PL_FILES => {},
);
