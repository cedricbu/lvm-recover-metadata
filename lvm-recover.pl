#!/usr/bin/perl
#
# Concern ? feedback ? question ? improvement request ? 
# Let me know!
#

# STATUS :
#  - FAIL : tell me ?
#  - WARN : We do not take into account when metadata wraps up back to beginning
#           This is currently impossible because we do not know the metadata size
#
# TODO :
#  - main while : have a continue block
#  - require Dump only when dumping
#  - storing/using creation_host
#  - Stop matching empty lines!
#  - make it work on older Perl versions
#  - Add optional full summary : each successful VG, print some data
#     - fully summary to contain readable time
#  - multi-level folder handling
#  - adding --help and --version <= --help doesn't work
#     - help : put note about TZ for when translating unix time to printable
#  - Should we warn or just step over non printable data ?
#  - Do we really need the out context ? and the others ?
#  - verifications : VGLooksGood(%VG) could be improved (checking value)
#  - Should we also search for PV header/labels ?



use 5.16.0;
use Getopt::Std;
use Fcntl qw( SEEK_SET SEEK_CUR );
use File::Basename;
use experimental qw(smartmatch);


### CONSTANTS ###

# represent contexts, that is wether we are supposed to be in 
# the root of the VG, in a PV, LV, LV's segment or segment's stripe
# Notes: 
# 	* Context will be generally pushed with '{' and popped with '}', or '[' and ']'
# 	* the LLV and LPV context represent the intermetiade context
# 	  within (physical|logical)_volumes { ... }
# 	* CTX_OUT : this is for data out of "vgname{ }"
# 	  However, in CTX_out, data are still stored in the current VG reference pointer
# 	* mirrors and stripes are handled in CTX_STP context
use constant { CTX_OUT => 0, CTX_VG => 1 , CTX_LPV => 2 , CTX_PV => 3, CTX_LLV => 4, 
	CTX_LV => 5 , CTX_SEG => 6, CTX_STP => 7};

### === Default Configuration === ###

my $max = 262144;  # stop after $max blocks (of $bsize bytes)[default : 128MB if block size is 512 bytes]
		   # this is to prevent going too far in case we read the block device directly) 
my $DEBUG = 0;  # debug level
my $namePattern = "%N_%T_%S.meta"; # used to generate a filename. see computeFileName() for the substitution
my $bsize = 512 ; # read chunk at a time. 1 sector, dont change unless you know wha tyou are doing
my $save = 1; # should the retrieved data stored in files ?
my $VERSION = "0.7.30"; # script version 



### === FUNCTIONS === ###

### Standard Opt first : Help and version
sub VERSION_MESSAGE {
	print "$0 version $VERSION\n";
	exit 0;
}
sub HELP_MESSAGE {
	print <<EOM;
Usage : $0 [-d[0-4]] [-f "filename pattern"] [-h] [--version] FILE

Will search into FILE (or in standard input if none provided) and extract all LVM2 metadata
it can find, and will store these in files defined with the '-f' option.

OPTIONS :
-n		Do not save retrieved data, exit directly after summary

-f "pattern"	Will store the successfully retrieve metadata in this pattern. There are several
		Meta-character :
			%N: VG name
			%S: seqno
			%T: creation_time
			%O: offset (in bytes) at which the data was found
			%I: VG UUID
		Modifier targs (see sprintf for more info) :
			.N : print at most N characters (useful for UUID and UUID-like names)
			0N : add leading 0s to get at least N digits (useful for sorting seqno)
		e.g. :
		if seqno is '3' and id is 'xxxxxx-yyyy'
		"%.6I_%04S" would give 'xxxxxx_0003'
			
		Default: "$namePattern"
		Notes : 
		 - if a file already exist, new one will be appended with _n, 'n' increased by 1
		until no such file exists
		 - You can create 1 layer of directory with '/', e.g. : "%.6I/%N_%T_%S"

-b[size]	specify the blocksize. Usually, default (usual size of a sector) should not be 
		touched.
		default: $bsize

-m[number]	Specify the maximum number of blocks to be read. Useful for avoiding going through
		the whole disk.
		Default: $max

-d[0-4]		Show debug info

-h		Show this message

--version	Show source version


FILE		source of the metadata. If this is not given, will read from standard input

EXAMPLES :
# dd if=/dev/sda bs=1M count=10 | gzip > sda.dd.gz
# zcat sda.dd.gz | $0

# $0 /dev/sda2


LIMITATION :
It will not wrap around the metadata area, since it does not know the area.
That is to say : if a metadata segment is cut and restarts at loops around at the begining
of the area, this metadata is likely to be considered as corrupted.

EOM
	exit 0;

}

## Do we have the mandatory bits ?
# - a creation_time, an ID, seqno etc.
sub VGLooksGood {
	my ($vgref) = @_;
	return 0 unless defined $vgref->{'creation_time'};
	return 0 unless defined $vgref->{'seqno'};
	return 0 unless defined $vgref->{'id'};
	return 0 unless $vgref->{'pv_count'} > 0;
	return 1;
}

## Do we need to keep that data for further reference ?
# Examples : OUT context : creation_time
#            VG  context : seqno
#
sub needsStoring {
	my ($context, $parameter) = @_;

	given ($context) {
		when (CTX_OUT) {
			return $parameter =~ /(creation_time)/;
		}
		when (CTX_VG) {
			return $parameter =~ /(id|seqno)/;

		}
	}
	# nothing worth keeping
	return 0;
}


# format the whole data
sub vgDataToStr {
	my ($whole) = @_;
	my $ret="";
	my $i=0 ; # indentation

	foreach(split(/\n/, $whole)) {
		$i-- if /^(}|\])$/;

		say "WARN: too many '}'" if $i < 0;

		$ret .= "\t" x $i . $_;
		## TODO : add the # dates, megabytes, etc.
		$ret .= "\n";

		$i++ if /({|\[)$/;

	}

	say "WARN: { and } do not match!" if $i != 0;
	return $ret;
}


# Takes a refervence to a metadata instance and a format
# Return the file name in which the metadata should be stored
#
# Substitution :
# S : seqno
# N : VG name
# T : creation_time
# O : offset
# I : VG UUID
# 
# Modifiers : see sprintf, but the most useful are :
# .N : print at most N characters
# 0N : add leading 0s to get at least N digits
# e.g. :
# if seqno is '3' and id is 'xxxx-yyyy'
# "%.4I_%04S" would give 'xxxx_0003'
#
# It's cool for sorting and keep controlably short file names!
sub computeFileName {
        my ($VGref, $fmt) = @_;
	my @args;

	# So: we are cheating a bit and use sprintf to add some control on the output.
	# It works as follow :
	#   - we catch all the %<fmt-control><key> one by one
	#      - <fmt-ctrl> being as described in the sprintf help
	#      - <key> being the reference to the entry (e.g. : [STONI])
	#   - we keep the fmt-ctrl as is and replace <key> by 's' ('string' in sprintf)
	#   - We push the corresponding key value in @args, the list that will be given to sprintf
	#   - when we're done, we simply call sprintf on format, with the @args with all the pushed values
	# That's cool, but it's quite **VERY** fragile :
	#   - we can't use 's' as key, otherwise it'll loop forever (thus all keys are now upper case to avoid conflicting with sprintf's 's')
	#   - try to go side ways and it might really do something very unexpected
	#

	while ($fmt =~ s/(%[0-9#-.*]*)(S|T|O|N|I)/\1s/c) {
		given($2) {
			when ('S') { push @args, $VGref->{'seqno'} }
			when ('T') { push @args, $VGref->{'creation_time'} }
			when ('O') { push @args, $VGref->{'offset'} }
			when ('N') { push @args, $VGref->{'name'} }
			when ('I') { push @args, $VGref->{'id'} }
		}
	}

	return sprintf( $fmt, @args );
}

### === END FUNCTIONS === ###







my %opts;
getopts('nm:hDd:f:', \%opts);
## Options :
# n  : Dry run, dont write files, exit after summary
# mX : Maximum number of blocks to read
# bX : Blocksize 
# d[0..5] : debug level
# D : Dump the whole Perl data structure
# f : file name format used for saving the data
# h : show help

HELP_MESSAGE() if defined( $opts{'h'} ) ;

## Time to assign the config values
$DEBUG = $opts{'d'} if defined( $opts{'d'} ) ;
$max = $opts{'m'} if defined( $opts{'m'} );
$bsize = $opts{'b'} if defined( $opts{'b'} );
$namePattern = $opts{'f'} if defined( $opts{'f'} );
$save = 0 if defined( $opts{'n'} );


### Main variables ###

## The hash : keys will be VG names (TODO: or should that change to UUID?)
#             value will be arrays of pointer to VG hashes
my (%VGs, %VGsIncomplete);
 
# current context (e.g. : VG, list of LVs, LV, PV, list of PVs, etc.)
# We need to keep track where we are to understand what we find.
# e.g. : a creation_time ? is it the VG's or an LV's 
my $context;  # current context

# The VG is represented by a hash, holding several data :
#  - name : name of the VG
#  - seqno : seqno of the instance
#  - creation_time : creation date of the instance
#  - id : VG UUID
#  - offset : where it was found on disk, in byte
#  - wholedata : the whole metadata string for this instance
#  - pv_count : number of PV (we need at least 1)
#
## Reference to the current instance of the VG.
my $VGInstanceRef;


## Where we are going to read from (standard file, block device or STDIN)
my $fh;

## Local/random variable
my $block;


say "DBG : debug level $DEBUG" if $DEBUG;

if ($ARGV[0]) {
	say "DBG : using $ARGV[0]" if $DEBUG;
	open($fh, "<", $ARGV[0]) or die ("can't open $ARGV[0] for reading : $!");
} else {
	say "DBG : standard input" if $DEBUG;
	$fh = *STDIN;
}


say "DBG: we are going to read at most " . $max * $bsize . " bytes" if $DEBUG >= 2;
# We will read the disk $bsize by $bsize (bsize REALLY should be sector, or we might miss something)


# Loop variables
my $offset = 0;
my $smthunexpected = 0; # error check (if non-zero, we will discard whatever we read)
my $nblocks = 0; # we keep track of the number of block read, to skip them all if we get

while ($offset < $max * $bsize) {  ## Until we go beyond our limits
## NOTE : There's a continue BLOCK at the end for the $offset increment

	## Reset the variables
	$nblocks = 0; # we keep track of the number of block read, to skip them all if we get
		      # a successful VG

	$smthunexpected = 0; # error check (if non-zero, we will discard whatever we read)

	printf "DBG : read offset 0x%x\n", $offset  if $DEBUG >= 2;
	
	# move to new offset
	seek $fh, $offset, SEEK_SET or last;

	# read the block, exit loop in case of EOF or read error
	read $fh, $block, $bsize or last;
	$nblocks++;

	# If it does not look like the begining of a VG, dont bother :
	#   Just set smthunexpected and go to next rount
	#
	# TODO : here we might want to check and have redirection to :
	#          - PV header"
	#          - ext/XFS file systems
	#          - LVM metadata
	#          - Partition table
	if ($block !~ m/^[[:word:]-]+ \{/ ) {
		$smthunexpected = 1;
		next;
	}

	say "DBG : offset $offset: we may have found VG called $1"  if $DEBUG >= 1;

	# Basically : if no null byte found, increase the length by bsize and search again
	# fill buffer until a null byte, we only need to read the last $bsize
	#
	# We keep track of the number of blocks read : if a VG is successfully retrieved,
	#   We dont want to reread its block again : it may lead to confusion (partial VG found)
	while ( index ( $block, "\x00", length($block) - $bsize ) == -1 ) {
		say "DBG: no null byte, it was not sufficient. current length : " . length( $block ) if $DEBUG >= 2;
		read $fh, $block, $bsize, length($block) or last;   # append into $block
		$nblocks++;
		say "DBG: after read : " . length( $block )  if $DEBUG >= 3;
	}
	## Now, we should have data that starts with a potential VG and has a null byte somewhere
	say "DBG: Now, we should have data that starts with a potential VG and has a null byte somewhere" if $DEBUG >= 3;

	## Discard the things after the null byte : we dont need it.
	($block) = split /\x00/, $block ;
	say "DBG: now we need to see if the following is a VG called $1" if $DEBUG >= 2;
	say $block if $DEBUG >= 4;
	say "DBG: Was it ?" if $DEBUG >= 4;
	
	# Does it look like beginning of metadata ? i.e. "VGname {"
	# Notes : 
	#   - the 'g' flag is there because we want the string position pointer to step in, lex-like.
	#   - $1 will have the VG name
	#   - in theory, we should not fail here since we checked that above already
	if ($block !~ m/^([[:word:]-]+) \{/g) {
                $smthunexpected = 1;
                next;
        }

	# We create a new instance of metadata, storing VG name, offset and entire string
	$VGInstanceRef = { name => $1, offset => $offset, wholedata => $block };
	

	# We are now positionned in the VG
	$context = CTX_VG;


	## Note : We already read the VG name previously. we're in already
	while ($block =~ m/(.*)/g ) {
		say "DBG : working on '$1'" if $DEBUG >= 3;
		given ($1) {
			## PART 0 : the ignores : empty lines and comments
			when (/^$/) {
				say "DBG: Dropping empty line" if $DEBUG >= 4;
			}
			when (/^[[:blank:]]*#/) {
				say "DBG: Dropping comment $1" if $DEBUG >= 4;
			}

			### PART 1 : Entering a contexts
			when (/physical_volumes \{/) {
				if ($context == CTX_VG) {
					say "DBG: Entering List PV context" if $DEBUG >= 3;
					$context = CTX_LPV;
				} else {
					say "Incorrect: hu, entering PV list unexpected from context $context" if $DEBUG; 
					$smthunexpected = 1;
				}
			}
			when (/logical_volumes \{/) {
				if ($context == CTX_VG) {
					say "DBG: Entering List LV context" if $DEBUG >= 3;
					$context = CTX_LLV;
				} else {
					say "Incorrect: hu, entering LV list unexpected from context $context" if $DEBUG; 
					$smthunexpected = 1;
				}
			}
			when (/segment([0-9]+) \{/) {
				if ($context == CTX_LV) {
					say "DBG: Entering segment context [$1]" if $DEBUG >= 3;
					$context = CTX_SEG;
				} else {
					say "Incorrect: hu, entering segment context unexpected from context $context" if $DEBUG; 
					$smthunexpected = 1;
				}
			}
			when (/^([[:graph:]]+) \{$/) { # 1 word (no space), followed by 1 space and a curly bracket
				given ($context) {
					when (CTX_LPV) {
						say "DBG: Entering PV context ($1)" if $DEBUG >= 3;
						$context = CTX_PV;
						$VGInstanceRef->{'pv_count'}++;
					}
					when (CTX_LLV) {
						say "DBG: Entering LV context ($1)" if $DEBUG >= 3;
						$context = CTX_LV;
					}
					default { say "Incorrect: hu, entering '$1' unexpected in context $context" if $DEBUG; $smthunexpected = 1 }
				}	
			}
			when (/^([[:graph:]]+) = \[$/) {
				# NOTE : treated as context despite the presence of '='
				#        thus it needs to be treated *BEFORE* the parameter handling
				#        This should be the 'stripe', 'mirror', and found in CTX_SEG only
				given ($context) {
					when (CTX_SEG) {
						say "DBG: Entering stripe context" if $DEBUG >= 3;
						$context = CTX_STP;
					}
					default { say "Incorrect: hu, entering stripe unexpected in context $context" if $DEBUG; $smthunexpected = 1 }
				}
			}

			## PART 2 : setting up parameters
			when (/([[:word:]]+)[[:blank:]]*=[[:blank:]]*([[:print:]]+)/) {
				say "DBG: CTX $context: '$1' = '$2'" if $DEBUG >= 3;
				if ( needsStoring($context, $1) ) {
					if (defined $VGInstanceRef->{$1}) {
						say "DBG: Incorrect, (context $context) $1 is wanted, but we had it already : " .
							"previous val : $VGInstanceRef->{$1}, " .
							"New val : $2" if $DEBUG;
						$smthunexpected = 1
					}
					say "DBG: (CTX $context) Storing : '$1' = '$2'" if $DEBUG >= 2;
					$VGInstanceRef->{$1} = $2 ;
					$VGInstanceRef->{$1} =~ s/"//g; # We need to cleanup the unwanted characters
				} else {
					say "DBG: '$1' and '$2' in context $context dont have to be stored" if $DEBUG >=3;
				}
			}
			when (/(\"[[:graph:]]+\"), ([[:digit:]]+)/) {
				given ($context) {
                                        when (CTX_STP) {
                                                say "DBG: stripe/mirror '$1' = '$2', ignored" if $DEBUG >= 3;
                                        }
                                        default { say "Incorrect hu, '$1, $2' unexpected in context $context" if $DEBUG; $smthunexpected = 1 }
                                }
			}

			## PART 3 : Exiting contexts
			when (/^}$/) { # popping context
				given ($context) {
					when (CTX_VG) {
						say "DBG: wrapping up VG : we're now out" if $DEBUG >= 3;
						$context = CTX_OUT;
					}
					when ([CTX_LLV, CTX_LPV]) {
						say "DBG: Back to VG context" if $DEBUG >= 3;
						$context = CTX_VG;
					}
					when (CTX_SEG) {
						say "DBG: Back to LV context" if $DEBUG >= 3;
						$context = CTX_LV;
					}
					when (CTX_LV) {
						say "DBG: Back to List LV context" if $DEBUG >= 3;
						$context = CTX_LLV;
					}
					when (CTX_PV) {
						say "DBG: Back to List PV context" if $DEBUG >= 3;
						$context = CTX_LPV;
					}
					default {
						say "ERR: hu, unexpected popping from $context with '}'"  if $DEBUG;
						$smthunexpected = 1;
					}
				}
			}
			when (/^]$/) { # popping stripe context
				given ($context) {
					when (CTX_STP) {
						say "DBG: Back to segment context" if $DEBUG >= 3;
						$context = CTX_SEG;
					}
					default {
						say "ERR: hu, unexpected popping from $context with ']'" if $DEBUG;
						$smthunexpected = 1;
					}
				}
			}
			default { say "ERR: hu, how to parse $1 ?" if $DEBUG; $smthunexpected = 1 }
		}
	}
	# reached the NULL byte or the End of Data for this block.
	

	# Does it look complete ?
	# We need to be in CTX_OUT context, and nothing was unexpected, and VG is considered to have basic data
	# otherwise it means we cut short.
	if ( $context == CTX_OUT && !$smthunexpected && VGLooksGood($VGInstanceRef) ) {
		# Success, we can consider the VG fully described and nothing was unexpected !
		say "DBG: Success, we have a full VG called $VGInstanceRef->{'name'} !!" if $DEBUG >= 2;
		# Push it in the list of good VGs.
		push @{ $VGs{ $VGInstanceRef->{'name'} } }, $VGInstanceRef;

	} else {
		# Push it in the list of bad VGs.
		say "DBG: $VGInstanceRef->{'name'} is not complete, adding to imcomplete list" if $DEBUG >= 2;
		push @{ $VGsIncomplete{ $VGInstanceRef->{'name'}} }, $VGInstanceRef ;

	}
	
} continue {
	## If there's success, we want to move by the number of blocks read, otherwise, only by 1 block
	if ($smthunexpected) {
		$offset += $bsize;
	} else {
		# There's no need to read the next $nblocks blocks : we have a successful VG and reading them
		# will get us confused with potential incomplete VGs
		
		$offset += $nblocks * $bsize;
	}

}


close $fh;
## PART ONE done : We're done with the file. now let's analyze.



say "SUMMMARY :";
say " - successfully found VGs :";
foreach my $vgname (keys %VGs) {
	my $size = @{$VGs{$vgname}};
	say "   - $size occurrence(s) of $vgname";
}
say " - VG with missing data :";
foreach my $vgname (keys %VGsIncomplete) {
	my $size = @{$VGsIncomplete{$vgname}};
        say "   - $size occurrence(s) of $vgname";
}

exit 0 unless $save;
say "SAVING" if $DEBUG;

foreach my $vgname (keys %VGs) {
	say "DBG: Saving data for $vgname" if $DEBUG >= 1;
	foreach my $instance (@{ $VGs{$vgname} }) {
		say "# seqno = $instance->{'seqno'}, creation time = $instance->{'creation_time'}" if $DEBUG >=3;
		my $filename = computeFileName($instance, $namePattern) ;
		my $dirname = dirname($filename);
		mkdir $dirname if not -d $dirname;
		if (-e $filename) {
			my $i = 1;
			$filename .= "_$i";
			until (not -e $filename) {
				$i++;
				$filename =~ s/_[0-9]+$/_$i/;
			}
		}
		say "Saving to $filename" if ($DEBUG >= 2);
		open (DEST, '>', $filename) or die "Could not create $filename: $!";
		print DEST vgDataToStr($instance->{'wholedata'}) or die "Could not write in $filename: $!";
		close DEST;
	}
}

say "DBG: Saving done" if $DEBUG;


